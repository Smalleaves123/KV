#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv/cluster/cluster_manager.h"
#include "kv/engine/db.h"
#include "kv/net/session.h"

namespace {

std::vector<std::string> Split(const std::string& line) {
  std::istringstream iss(line);
  std::vector<std::string> out;
  std::string token;
  while (iss >> token) out.push_back(token);
  return out;
}

bool ParseBulk(const std::string& resp, size_t* pos, std::string* out);

bool ParseLineToken(const std::string& resp, size_t* pos, std::string* out) {
  if (pos == nullptr || out == nullptr) {
    return false;
  }
  const size_t start = *pos;
  const size_t end = resp.find("\r\n", start);
  if (end == std::string::npos) {
    return false;
  }
  *out = resp.substr(start, end - start);
  *pos = end + 2;
  return true;
}

bool ParseArray(const std::string& resp, size_t* pos, std::string* out) {
  std::string cnt_s;
  if (!ParseLineToken(resp, pos, &cnt_s)) {
    return false;
  }

  int count = 0;
  try {
    count = std::stoi(cnt_s);
  } catch (...) {
    return false;
  }

  std::ostringstream oss;
  for (int i = 0; i < count; ++i) {
    std::string item;
    if (!ParseBulk(resp, pos, &item)) {
      return false;
    }
    oss << item;
    if (i + 1 < count) {
      oss << '\n';
    }
  }
  *out = oss.str();
  return true;
}

bool ParseBulk(const std::string& resp, size_t* pos, std::string* out) {
  if (*pos >= resp.size()) {
    return false;
  }

  const char prefix = resp[*pos];
  ++(*pos);

  if (prefix == '+') {
    return ParseLineToken(resp, pos, out);
  }

  if (prefix == '-') {
    std::string err;
    if (!ParseLineToken(resp, pos, &err)) {
      return false;
    }
    *out = "error: " + err;
    return true;
  }

  if (prefix == '$') {
    std::string len_s;
    if (!ParseLineToken(resp, pos, &len_s)) {
      return false;
    }

    int len = 0;
    try {
      len = std::stoi(len_s);
    } catch (...) {
      return false;
    }

    if (len < 0) {
      *out = "(nil)";
      return true;
    }

    if (*pos + static_cast<size_t>(len) + 2 > resp.size()) {
      return false;
    }

    *out = resp.substr(*pos, static_cast<size_t>(len));
    *pos += static_cast<size_t>(len) + 2;
    return true;
  }

  if (prefix == '*') {
    return ParseArray(resp, pos, out);
  }

  return false;
}

std::string HumanizeResp(const std::string& resp) {
  size_t pos = 0;
  std::string out;
  if (!ParseBulk(resp, &pos, &out)) {
    return resp;
  }
  return out;
}

void PrintHelp() {
  std::cout
      << "Commands:\n"
      << "  get <key>\n"
      << "  set <key> <value>\n"
      << "  del <key>\n"
      << "  mget <k1> <k2> ...\n"
      << "  ping\n"
      << "  info\n"
      << "  stats\n"
      << "  cluster route|status|batch\n"
      << "  snap create <name>\n"
      << "  snap get <name> <key>\n"
      << "  snap release <name>\n"
      << "  cache\n"
      << "  txn begin|exec|abort\n"
      << "  help\n"
      << "  quit\n";
}
}  // namespace

int main(int argc, char** argv) {
  kv::DBOptions options;
  if (argc > 1) options.db_path = argv[1];

  std::unique_ptr<kv::DB> db;
  kv::Status s = kv::DB::Open(options, &db);
  if (!s.ok()) {
    std::cerr << "open failed: " << s.ToString() << "\n";
    return 1;
  }

  kv::ClusterManager cluster_manager(8);
  (void)cluster_manager.AddNode(
      kv::NodeInfo{"local", "127.0.0.1", 1, 1, true});
  cluster_manager.SetLocalNodeId("local");
  kv::net::Session session(db.get(), &cluster_manager);

  std::unordered_map<std::string, const kv::Snapshot*> snapshots;

  std::cout << "kv_cli ready. type 'help'.\n";
  std::string line;
  while (true) {
    std::cout << "kv> ";
    if (!std::getline(std::cin, line)) break;
    const auto args = Split(line);
    if (args.empty()) continue;

    const std::string& cmd = args[0];
    if (cmd == "quit" || cmd == "exit") break;

    if (cmd == "help") {
      PrintHelp();
      continue;
    }

    if (cmd == "snap") {
      if (args.size() < 3) {
        std::cout << "usage: snap create|get|release ...\n";
        continue;
      }

      if (args[1] == "create") {
        const std::string& name = args[2];
        if (snapshots.count(name)) {
          std::cout << "snapshot exists: " << name << "\n";
          continue;
        }
        const kv::Snapshot* snap = db->GetSnapshot();
        if (snap == nullptr) {
          std::cout << "error: create snapshot failed\n";
          continue;
        }
        snapshots[name] = snap;
        std::cout << "snapshot " << name << " created\n";
        continue;
      }

      if (args[1] == "get") {
        if (args.size() != 4) {
          std::cout << "usage: snap get <name> <key>\n";
          continue;
        }
        auto it = snapshots.find(args[2]);
        if (it == snapshots.end()) {
          std::cout << "snapshot not found: " << args[2] << "\n";
          continue;
        }

        kv::ReadOptions ro;
        ro.snapshot = it->second;

        std::string v;
        s = db->Get(ro, args[3], &v);
        if (s.ok()) std::cout << v << "\n";
        else if (s.IsNotFound()) std::cout << "(nil)\n";
        else std::cout << "error: " << s.ToString() << "\n";
        continue;
      }

      if (args[1] == "release") {
        const std::string& name = args[2];
        auto it = snapshots.find(name);
        if (it == snapshots.end()) {
          std::cout << "snapshot not found: " << name << "\n";
          continue;
        }
        s = db->ReleaseSnapshot(it->second);
        if (!s.ok()) std::cout << "error: " << s.ToString() << "\n";
        else {
          snapshots.erase(it);
          std::cout << "released " << name << "\n";
        }
        continue;
      }

      std::cout << "unknown snap command\n";
      continue;
    }
    if (cmd == "stats") {
      kv::CacheStats st;
      s = db->GetCacheStats(&st);
      if (!s.ok()) {
        std::cout << "error: " << s.ToString() << "\n";
      } else {
        std::cout << "cache_hit=" << st.hit
                  << " cache_miss=" << st.miss
                  << " cache_evict=" << st.evict
                  << " cache_expire=" << st.expire << "\n";
      }
      continue;
    }

    if (cmd == "cache") {
      kv::CacheStats st;
      s = db->GetCacheStats(&st);
      if (!s.ok()) {
        std::cout << "error: " << s.ToString() << "\n";
      } else {
        std::cout << "cache_hit=" << st.hit
                  << " cache_miss=" << st.miss
                  << " cache_evict=" << st.evict
                  << " cache_expire=" << st.expire << "\n";
      }
      continue;
    }

    if (cmd == "txn") {
      if (args.size() != 2) {
        std::cout << "usage: txn begin|exec|abort\n";
        continue;
      }
      if (args[1] == "begin") {
        std::cout << HumanizeResp(session.HandleLine("BEGIN")) << "\n";
        continue;
      }
      if (args[1] == "exec") {
        std::cout << HumanizeResp(session.HandleLine("EXEC")) << "\n";
        continue;
      }
      if (args[1] == "abort") {
        std::cout << HumanizeResp(session.HandleLine("ABORT")) << "\n";
        continue;
      }
      std::cout << "usage: txn begin|exec|abort\n";
      continue;
    }


    const std::string encoded = session.HandleLine(line);
    std::cout << HumanizeResp(encoded) << "\n";
  }

  for (const auto& kvp : snapshots) {
    (void)db->ReleaseSnapshot(kvp.second);
  }
  (void)db->Close();
  return 0;
}
