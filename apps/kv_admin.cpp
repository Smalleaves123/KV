// apps/kv_admin.cpp
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "kv/engine/db.h"
#include "kv/version/manifest.h"

namespace fs = std::filesystem;

namespace {

void PrintUsage() {
  std::cout
      << "Usage:\n"
      << "  kv_admin status <db_path>\n"
      << "  kv_admin stats <db_path>\n"
      << "  kv_admin compact <db_path>\n"
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

std::pair<size_t, uint64_t> SstCountAndBytes(const std::string& sst_dir) {
  std::error_code ec;
  if (!fs::exists(sst_dir, ec) || !fs::is_directory(sst_dir, ec)) {
    return {0, 0};
  }

  size_t count = 0;
  uint64_t bytes = 0;
  for (const auto& e : fs::directory_iterator(sst_dir, ec)) {
    if (ec) {
      break;
    }
    if (!e.is_regular_file() || e.path().extension() != ".sst") {
      continue;
    }
    ++count;
    bytes += static_cast<uint64_t>(e.file_size());
  }
  return {count, bytes};
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

void CmdStats(const std::string& db_path) {
  kv::DBOptions options;
  options.db_path = db_path;
  options.create_if_missing = false;

  std::unique_ptr<kv::DB> db;
  kv::Status s = kv::DB::Open(options, &db);
  if (!s.ok()) {
    std::cout << "open db failed: " << s.ToString() << "\n";
    return;
  }

  kv::CacheStats cache_stats;
  s = db->GetCacheStats(&cache_stats);
  if (!s.ok()) {
    std::cout << "get cache stats failed: " << s.ToString() << "\n";
    (void)db->Close();
    return;
  }

  kv::ReadPathStats read_stats;
  s = db->GetReadPathStats(&read_stats);
  if (!s.ok()) {
    std::cout << "get read path stats failed: " << s.ToString() << "\n";
    (void)db->Close();
    return;
  }

  kv::CompactionStats compaction_stats;
  s = db->GetCompactionStats(&compaction_stats);
  if (!s.ok()) {
    std::cout << "get compaction stats failed: " << s.ToString() << "\n";
    (void)db->Close();
    return;
  }

  std::cout << "cache.hit            : " << cache_stats.hit << "\n";
  std::cout << "cache.miss           : " << cache_stats.miss << "\n";
  std::cout << "cache.evict          : " << cache_stats.evict << "\n";
  std::cout << "cache.expire         : " << cache_stats.expire << "\n";
  std::cout << "read.table_cache_hits: " << read_stats.table_cache_hits << "\n";
  std::cout << "read.table_cache_misses: " << read_stats.table_cache_misses
            << "\n";
  std::cout << "read.table_cache_evictions: "
            << read_stats.table_cache_evictions << "\n";
  std::cout << "read.table_cache_entries: " << read_stats.table_cache_entries
            << "\n";
  std::cout << "read.bloom_queries   : " << read_stats.bloom_queries
            << "\n";
  std::cout << "read.bloom_negatives : " << read_stats.bloom_negatives
            << "\n";
  std::cout << "compact.trigger      : " << compaction_stats.trigger_attempts << "\n";
  std::cout << "compact.skip_snapshot: " << compaction_stats.skipped_due_snapshot
            << "\n";
  std::cout << "compact.skip_threshold: " << compaction_stats.skipped_due_threshold
            << "\n";
  std::cout << "compact.success      : " << compaction_stats.succeeded << "\n";
  std::cout << "compact.failed       : " << compaction_stats.failed << "\n";

  (void)db->Close();
}

void CmdCompact(const std::string& db_path) {
  kv::DBOptions options;
  options.db_path = db_path;
  options.create_if_missing = false;

  std::unique_ptr<kv::DB> db;
  kv::Status s = kv::DB::Open(options, &db);
  if (!s.ok()) {
    std::cout << "open db failed: " << s.ToString() << "\n";
    return;
  }

  const std::string sst_dir = SstDir(db_path);
  const auto before = SstCountAndBytes(sst_dir);

  s = db->Compact();
  if (!s.ok()) {
    std::cout << "compact failed: " << s.ToString() << "\n";
    (void)db->Close();
    return;
  }

  const auto after = SstCountAndBytes(sst_dir);
  std::cout << "compact ok\n";
  std::cout << "sst_count_before: " << before.first << "\n";
  std::cout << "sst_bytes_before: " << before.second << "\n";
  std::cout << "sst_count_after : " << after.first << "\n";
  std::cout << "sst_bytes_after : " << after.second << "\n";
  (void)db->Close();
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
  else if (cmd == "stats") CmdStats(db_path);
  else if (cmd == "compact") CmdCompact(db_path);
  else if (cmd == "list-sst") CmdListSst(db_path);
  else if (cmd == "manifest-dump") CmdManifestDump(db_path);
  else {
    PrintUsage();
    return 1;
  }

  return 0;
}
