// apps/kv_admin.cpp
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "kv/version/manifest.h"

namespace fs = std::filesystem;

namespace {

void PrintUsage() {
  std::cout
      << "Usage:\n"
      << "  kv_admin status <db_path>\n"
      << "  kv_admin list-sst <db_path>\n"
      << "  kv_admin manifest-dump <db_path>\n";
}

std::string WalPath(const std::string& db_path) {
  return db_path + "/wal.log";
}

std::string SstDir(const std::string& db_path) {
  return db_path + "/sst";
}

std::string ManifestPath(const std::string& db_path) {
  return db_path + "/MANIFEST";
}

void CmdStatus(const std::string& db_path) {
  std::error_code ec;
  if (!fs::exists(db_path, ec)) {
    std::cout << "db_path not found: " << db_path << "\n";
    return;
  }

  const std::string wal = WalPath(db_path);
  const std::string sst_dir = SstDir(db_path);
  const std::string manifest = ManifestPath(db_path);

  std::cout << "db_path      : " << db_path << "\n";
  std::cout << "wal_path     : " << wal << "\n";
  std::cout << "manifest     : " << manifest << "\n";
  std::cout << "sst_dir      : " << sst_dir << "\n";

  if (fs::exists(wal, ec)) {
    std::cout << "wal_exists   : yes\n";
    std::cout << "wal_size     : " << fs::file_size(wal, ec) << "\n";
  } else {
    std::cout << "wal_exists   : no\n";
  }

  size_t sst_count = 0;
  if (fs::exists(sst_dir, ec) && fs::is_directory(sst_dir, ec)) {
    for (const auto& e : fs::directory_iterator(sst_dir, ec)) {
      if (e.is_regular_file() && e.path().extension() == ".sst") ++sst_count;
    }
  }
  std::cout << "sst_count    : " << sst_count << "\n";
}

void CmdListSst(const std::string& db_path) {
  const std::string sst_dir = SstDir(db_path);
  std::error_code ec;
  if (!fs::exists(sst_dir, ec)) {
    std::cout << "no sst dir\n";
    return;
  }

  for (const auto& e : fs::directory_iterator(sst_dir, ec)) {
    if (!e.is_regular_file() || e.path().extension() != ".sst") continue;
    std::cout << e.path().string() << "\n";
  }
}

void CmdManifestDump(const std::string& db_path) {
  kv::Manifest manifest;
  const std::string path = ManifestPath(db_path);

  kv::Status s = manifest.Open(path, false);
  if (!s.ok()) {
    std::cout << "open manifest failed: " << s.ToString() << "\n";
    return;
  }

  std::vector<kv::ManifestFileMeta> files;
  s = manifest.Recover(&files);
  if (!s.ok()) {
    std::cout << "recover manifest failed: " << s.ToString() << "\n";
    (void)manifest.Close();
    return;
  }

  std::cout << "records: " << files.size() << "\n";
  for (const auto& f : files) {
    std::cout << "file_number=" << f.file_number
              << " max_seq=" << f.max_seq
              << " path=" << f.file_path << "\n";
  }

  (void)manifest.Close();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    PrintUsage();
    return 1;
  }

  const std::string cmd = argv[1];
  const std::string db_path = argv[2];

  if (cmd == "status") CmdStatus(db_path);
  else if (cmd == "list-sst") CmdListSst(db_path);
  else if (cmd == "manifest-dump") CmdManifestDump(db_path);
  else {
    PrintUsage();
    return 1;
  }

  return 0;
}
