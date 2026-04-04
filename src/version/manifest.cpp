#include "kv/version/manifest.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <system_error>

namespace kv {
namespace {

enum class ManifestTag : uint8_t {
  kAddFile = 1,
};

void AppendFixed32(std::string* out, uint32_t value) {
  out->push_back(static_cast<char>(value & 0xFF));
  out->push_back(static_cast<char>((value >> 8) & 0xFF));
  out->push_back(static_cast<char>((value >> 16) & 0xFF));
  out->push_back(static_cast<char>((value >> 24) & 0xFF));
}

void AppendFixed64(std::string* out, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

uint32_t DecodeFixed32(const char* p) {
  return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

uint64_t DecodeFixed64(const char* p) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= (static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i));
  }
  return value;
}

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

}  // namespace

Manifest::Manifest() : append_stream_(), file_path_(), is_open_(false) {}

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

  file_path_ = file_path;
  is_open_ = true;
  return Status::OK();
}

Status Manifest::Close() {
  if (!append_stream_.is_open()) {
    append_stream_.clear();
    is_open_ = false;
    return Status::OK();
  }

  append_stream_.flush();
  if (!append_stream_) {
    append_stream_.close();
    append_stream_.clear();
    is_open_ = false;
    return Status::IOError("failed to flush manifest before close");
  }

  append_stream_.close();
  if (append_stream_.fail()) {
    append_stream_.clear();
    is_open_ = false;
    return Status::IOError("failed to close manifest file");
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
  AppendFixed64(&record, file_meta.file_number);
  AppendFixed64(&record, file_meta.max_seq);
  AppendFixed32(&record, static_cast<uint32_t>(file_meta.file_path.size()));
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

    if (static_cast<uint8_t>(tag_raw) != static_cast<uint8_t>(ManifestTag::kAddFile)) {
      return Status::Corruption("unknown manifest tag");
    }

    std::array<char, 20> header{};
    in.read(header.data(), static_cast<std::streamsize>(header.size()));
    if (in.gcount() != static_cast<std::streamsize>(header.size())) {
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
      return Status::Corruption("truncated manifest add-file path");
    }

    files->push_back(std::move(meta));
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
