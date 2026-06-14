#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "kv/engine/db.h"

namespace {

struct Options {
  std::string db_path = "test_tmp/bench/db";
  std::string workload = "mixed";
  int operations = 10000;
  int value_size = 100;
  int read_percent = 80;
  bool cache = false;
  bool json = true;
};

std::string RepeatValue(int size, int seed) {
  std::string value;
  value.reserve(static_cast<size_t>(size));
  for (int i = 0; i < size; ++i) {
    value.push_back(static_cast<char>('a' + ((i + seed) % 26)));
  }
  return value;
}

bool ParseInt(const char* s, int* out) {
  if (s == nullptr || out == nullptr) return false;
  char* end = nullptr;
  const long v = std::strtol(s, &end, 10);
  if (end == s || *end != '\0') return false;
  *out = static_cast<int>(v);
  return true;
}

Options ParseArgs(int argc, char** argv) {
  Options opt;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto read_value = [&](std::string* value) {
      if (i + 1 >= argc) return false;
      *value = argv[++i];
      return true;
    };
    auto read_int = [&](int* value) {
      if (i + 1 >= argc) return false;
      return ParseInt(argv[++i], value);
    };

    if (arg == "--db") {
      (void)read_value(&opt.db_path);
    } else if (arg == "--workload") {
      (void)read_value(&opt.workload);
    } else if (arg == "--ops") {
      (void)read_int(&opt.operations);
    } else if (arg == "--value-size") {
      (void)read_int(&opt.value_size);
    } else if (arg == "--read-percent") {
      (void)read_int(&opt.read_percent);
    } else if (arg == "--cache") {
      opt.cache = true;
    } else if (arg == "--text") {
      opt.json = false;
    }
  }
  if (opt.operations < 1) opt.operations = 1;
  if (opt.value_size < 0) opt.value_size = 0;
  if (opt.read_percent < 0) opt.read_percent = 0;
  if (opt.read_percent > 100) opt.read_percent = 100;
  return opt;
}

void PrepareDir(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  std::filesystem::create_directories(path, ec);
}

kv::DBOptions MakeOptions(const Options& opt) {
  kv::DBOptions db_options;
  db_options.db_path = opt.db_path;
  db_options.cache_enabled = opt.cache;
  db_options.cache_capacity = 8192;
  db_options.memtable_write_buffer_size = 64 * 1024;
  db_options.auto_compaction_enabled = true;
  return db_options;
}

struct Result {
  std::string workload;
  int operations = 0;
  double seconds = 0.0;
  double ops_per_sec = 0.0;
  kv::CacheStats cache;
  kv::ReadPathStats read_path;
  kv::CompactionStats compaction;
};

void SeedData(kv::DB* db, int count, int value_size) {
  for (int i = 0; i < count; ++i) {
    const std::string key = "k" + std::to_string(i);
    const std::string value = RepeatValue(value_size, i);
    const kv::Status s = db->Put(kv::WriteOptions{}, key, value);
    if (!s.ok()) {
      std::cerr << "seed put failed: " << s.ToString() << "\n";
      std::exit(2);
    }
  }
}

Result Run(const Options& opt) {
  PrepareDir(opt.db_path);

  std::unique_ptr<kv::DB> db;
  kv::Status s = kv::DB::Open(MakeOptions(opt), &db);
  if (!s.ok()) {
    std::cerr << "open failed: " << s.ToString() << "\n";
    std::exit(1);
  }

  const int key_space = std::max(1, opt.operations / 2);
  if (opt.workload == "read" || opt.workload == "mixed" ||
      opt.workload == "negative-read") {
    SeedData(db.get(), key_space, opt.value_size);
    s = db->Compact();
    if (!s.ok() && !s.IsNotFound()) {
      std::cerr << "seed flush failed: " << s.ToString() << "\n";
      std::exit(2);
    }
  }

  std::mt19937 rng(12345);
  std::uniform_int_distribution<int> key_dist(0, key_space - 1);
  std::uniform_int_distribution<int> pct_dist(1, 100);

  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < opt.operations; ++i) {
    const bool do_read =
        opt.workload == "read" ||
        (opt.workload == "mixed" && pct_dist(rng) <= opt.read_percent) ||
        opt.workload == "negative-read";

    if (do_read) {
      std::string value;
      std::string key;
      if (opt.workload == "negative-read") {
        key = "missing" + std::to_string(i);
      } else {
        key = "k" + std::to_string(key_dist(rng));
      }
      s = db->Get(kv::ReadOptions{}, key, &value);
      if (!s.ok() && !s.IsNotFound()) {
        std::cerr << "get failed: " << s.ToString() << "\n";
        std::exit(2);
      }
      continue;
    }

    const std::string key = "k" + std::to_string(i % key_space);
    const std::string value = RepeatValue(opt.value_size, i);
    s = db->Put(kv::WriteOptions{}, key, value);
    if (!s.ok()) {
      std::cerr << "put failed: " << s.ToString() << "\n";
      std::exit(2);
    }
  }
  const auto end = std::chrono::steady_clock::now();

  Result result;
  result.workload = opt.workload;
  result.operations = opt.operations;
  result.seconds = std::chrono::duration<double>(end - start).count();
  result.ops_per_sec =
      result.seconds == 0.0 ? 0.0 : opt.operations / result.seconds;
  (void)db->GetCacheStats(&result.cache);
  (void)db->GetReadPathStats(&result.read_path);
  (void)db->GetCompactionStats(&result.compaction);
  (void)db->Close();
  return result;
}

void PrintJson(const Result& r) {
  std::cout << "{\n"
            << "  \"workload\": \"" << r.workload << "\",\n"
            << "  \"operations\": " << r.operations << ",\n"
            << "  \"seconds\": " << r.seconds << ",\n"
            << "  \"ops_per_sec\": " << r.ops_per_sec << ",\n"
            << "  \"cache\": {\n"
            << "    \"hit\": " << r.cache.hit << ",\n"
            << "    \"miss\": " << r.cache.miss << ",\n"
            << "    \"evict\": " << r.cache.evict << ",\n"
            << "    \"expire\": " << r.cache.expire << "\n"
            << "  },\n"
            << "  \"read_path\": {\n"
            << "    \"table_cache_hits\": " << r.read_path.table_cache_hits
            << ",\n"
            << "    \"table_cache_misses\": "
            << r.read_path.table_cache_misses << ",\n"
            << "    \"bloom_queries\": " << r.read_path.bloom_queries
            << ",\n"
            << "    \"bloom_negatives\": " << r.read_path.bloom_negatives
            << "\n"
            << "  },\n"
            << "  \"compaction\": {\n"
            << "    \"trigger_attempts\": "
            << r.compaction.trigger_attempts << ",\n"
            << "    \"succeeded\": " << r.compaction.succeeded << ",\n"
            << "    \"failed\": " << r.compaction.failed << "\n"
            << "  }\n"
            << "}\n";
}

void PrintText(const Result& r) {
  std::cout << "workload=" << r.workload
            << " operations=" << r.operations
            << " seconds=" << r.seconds
            << " ops_per_sec=" << r.ops_per_sec
            << " cache_hit=" << r.cache.hit
            << " cache_miss=" << r.cache.miss
            << " bloom_queries=" << r.read_path.bloom_queries
            << " bloom_negatives=" << r.read_path.bloom_negatives
            << " compaction_success=" << r.compaction.succeeded << "\n";
}

}  // namespace

int main(int argc, char** argv) {
  const Options opt = ParseArgs(argc, argv);
  const Result result = Run(opt);
  if (opt.json) {
    PrintJson(result);
  } else {
    PrintText(result);
  }
  return 0;
}
