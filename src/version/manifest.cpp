#include "kv/version/manifest.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <system_error>

#include "kv/common/encoding.h"
#include "kv/common/file_compat.h"

namespace kv {
namespace {

enum class ManifestTag : uint8_t {
  kAddFile = 1,
  kRemoveFile = 2,
};

Status EnsureParentDirectory(const std::filesystem::path& path) {
  const auto parent = path.parent_path();
  if (parent.empty()) {
    return Status::OK();
  }

  std::error_code ec;
  std::filesystem::create_directories(parent, ec);
  if (ec) {
    return Status::IOError("failed to create manifest directory: " + parent.string());
  }

  return Status::OK();
}

bool IsTrailingShortRead(const std::ifstream& in) {
  return in.eof();
}

}  // namespace

Manifest::Manifest()
    : append_stream_(), file_path_(), sync_fd_(-1), is_open_(false) {}

Manifest::~Manifest() {
  (void)Close();
}

Status Manifest::Open(const std::string& file_path, bool create_if_missing) {
  if (file_path.empty()) {
    return Status::InvalidArgument("manifest path is empty");
  }

  Status s = Close();
  if (!s.ok()) {
    return s;
  }

  const std::filesystem::path path(file_path);
  s = EnsureParentDirectory(path);
  if (!s.ok()) {
    return s;
  }

  std::error_code ec;
  const bool exists = std::filesystem::exists(path, ec);
  if (ec) {
    return Status::IOError("failed to query manifest path: " + file_path);
  }

  if (!exists && !create_if_missing) {
    return Status::NotFound("manifest file does not exist: " + file_path);
  }

  append_stream_.clear();
  append_stream_.open(file_path, std::ios::binary | std::ios::app);
  if (!append_stream_.is_open()) {
    append_stream_.clear();
    return Status::IOError("failed to open manifest file: " + file_path);
  }

  sync_fd_ = platform::OpenSyncFile(file_path, true);
  if (sync_fd_ < 0) {
    const std::string err = platform::FileErrorString();
    append_stream_.close();
    append_stream_.clear();
    return Status::IOError("failed to open manifest fd for sync: " + file_path +
                           ": " + err);
  }

  file_path_ = file_path;
  is_open_ = true;
  return Status::OK();
}

Status Manifest::Close() {
  if (!append_stream_.is_open()) {
    if (sync_fd_ >= 0) {
      (void)platform::CloseFile(sync_fd_);
      sync_fd_ = -1;
    }
    append_stream_.clear();
    is_open_ = false;
    return Status::OK();
  }

  Status status = Sync();

  append_stream_.close();
  if (append_stream_.fail() && status.ok()) {
    status = Status::IOError("failed to close manifest file");
  }
  if (sync_fd_ >= 0 && platform::CloseFile(sync_fd_) != 0 && status.ok()) {
    status = Status::IOError("failed to close manifest sync fd: " +
                             platform::FileErrorString());
  }
  sync_fd_ = -1;
  if (!status.ok()) {
    append_stream_.clear();
    is_open_ = false;
    return status;
  }

  append_stream_.clear();
  is_open_ = false;
  return Status::OK();
}

Status Manifest::AddFile(const ManifestFileMeta& file_meta) {
  if (!is_open_ || !append_stream_.is_open()) {
    return Status::IOError("manifest is not open");
  }
  if (file_meta.file_number == 0) {
    return Status::InvalidArgument("manifest file number must be greater than 0");
  }
  if (file_meta.file_path.empty()) {
    return Status::InvalidArgument("manifest file path is empty");
  }

  std::string record;
  record.reserve(1 + 8 + 8 + 4 + file_meta.file_path.size());
  record.push_back(static_cast<char>(ManifestTag::kAddFile));
  EncodeFixed64(&record, file_meta.file_number);
  EncodeFixed64(&record, file_meta.max_seq);
  EncodeFixed32(&record, static_cast<uint32_t>(file_meta.file_path.size()));
  record.append(file_meta.file_path);

  append_stream_.write(record.data(), static_cast<std::streamsize>(record.size()));
  if (!append_stream_) {
    return Status::IOError("failed to append manifest record");
  }

  append_stream_.flush();
  if (!append_stream_) {
    return Status::IOError("failed to flush manifest record");
  }

  return Status::OK();
}

Status Manifest::Sync() {
  if (!is_open_ || !append_stream_.is_open()) {
    return Status::IOError("manifest is not open");
  }

  append_stream_.flush();
  if (!append_stream_) {
    return Status::IOError("failed to flush manifest records");
  }

  if (sync_fd_ < 0 || platform::SyncFile(sync_fd_) != 0) {
    return Status::IOError("failed to sync manifest file: " +
                           platform::FileErrorString());
  }

  return Status::OK();
}

Status Manifest::RemoveFile(uint64_t file_number) {
  if (!is_open_ || !append_stream_.is_open()) {
    return Status::IOError("manifest is not open");
  }
  if (file_number == 0) {
    return Status::InvalidArgument("manifest file number must be greater than 0");
  }

  std::string record;
  record.reserve(1 + 8);
  record.push_back(static_cast<char>(ManifestTag::kRemoveFile));
  EncodeFixed64(&record, file_number);

  append_stream_.write(record.data(), static_cast<std::streamsize>(record.size()));
  if (!append_stream_) {
    return Status::IOError("failed to append manifest remove-file record");
  }

  append_stream_.flush();
  if (!append_stream_) {
    return Status::IOError("failed to flush manifest remove-file record");
  }

  return Status::OK();
}

Status Manifest::Recover(std::vector<ManifestFileMeta>* files) const {
  if (files == nullptr) {
    return Status::InvalidArgument("manifest recover output is null");
  }
  files->clear();

  if (file_path_.empty()) {
    return Status::InvalidArgument("manifest path is empty");
  }

  std::ifstream in(file_path_, std::ios::binary);
  if (!in.is_open()) {
    return Status::IOError("failed to open manifest for recovery: " + file_path_);
  }

  std::vector<uint64_t> order;
  std::unordered_map<uint64_t, ManifestFileMeta> latest;
  std::unordered_map<uint64_t, size_t> order_index;

  while (true) {
    char tag_raw = 0;
    in.read(&tag_raw, 1);
    if (in.gcount() == 0) {
      in.clear();
      break;
    }
    if (in.gcount() != 1) {
      return Status::Corruption("truncated manifest tag");
    }

    const ManifestTag tag = static_cast<ManifestTag>(static_cast<uint8_t>(tag_raw));
    if (tag == ManifestTag::kAddFile) {
      std::array<char, 20> header{};
      in.read(header.data(), static_cast<std::streamsize>(header.size()));
      if (in.gcount() != static_cast<std::streamsize>(header.size())) {
        if (IsTrailingShortRead(in)) {
          break;
        }
        return Status::Corruption("truncated manifest add-file header");
      }

      ManifestFileMeta meta;
      meta.file_number = DecodeFixed64(header.data());
      meta.max_seq = DecodeFixed64(header.data() + 8);
      const uint32_t path_len = DecodeFixed32(header.data() + 16);
      if (path_len == 0) {
        return Status::Corruption("manifest add-file path is empty");
      }

      meta.file_path.assign(path_len, '\0');
      in.read(meta.file_path.data(), static_cast<std::streamsize>(path_len));
      if (in.gcount() != static_cast<std::streamsize>(path_len)) {
        if (IsTrailingShortRead(in)) {
          break;
        }
        return Status::Corruption("truncated manifest add-file path");
      }

      if (order_index.find(meta.file_number) == order_index.end()) {
        order_index.emplace(meta.file_number, order.size());
        order.push_back(meta.file_number);
      }
      latest[meta.file_number] = std::move(meta);
      continue;
    }

    if (tag == ManifestTag::kRemoveFile) {
      std::array<char, 8> payload{};
      in.read(payload.data(), static_cast<std::streamsize>(payload.size()));
      if (in.gcount() != static_cast<std::streamsize>(payload.size())) {
        if (IsTrailingShortRead(in)) {
          break;
        }
        return Status::Corruption("truncated manifest remove-file payload");
      }
      const uint64_t file_number = DecodeFixed64(payload.data());
      latest.erase(file_number);
      continue;
    }

    return Status::Corruption("unknown manifest tag");
  }

  for (uint64_t file_number : order) {
    auto it = latest.find(file_number);
    if (it != latest.end()) {
      files->push_back(it->second);
    }
  }

  return Status::OK();
}

bool Manifest::IsOpen() const noexcept {
  return is_open_ && append_stream_.is_open();
}

const std::string& Manifest::file_path() const noexcept {
  return file_path_;
}

}  // namespace kv
