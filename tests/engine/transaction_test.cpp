#include "kv/engine/db.h"

#include <filesystem>
#include <memory>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

namespace kv {
namespace {

DBOptions MakeTxnDBOptions(const std::string& name) {
  static int counter = 0;
  ++counter;

  DBOptions options;
  std::ostringstream oss;
  oss << "test_tmp/db/" << name << "_" << counter;
  const std::string base = oss.str();

  options.wal_path = base + ".wal";
  options.sst_dir = base + "_sst";
  options.manifest_path = base + ".manifest";
  options.memtable_write_buffer_size = 1ULL << 20;
  return options;
}

void RemovePathIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
}

void RemoveDirIfExists(const std::string& dir_path) {
  std::error_code ec;
  std::filesystem::remove_all(dir_path, ec);
}

std::unique_ptr<DB> OpenTxnDB(const std::string& name) {
  DBOptions options = MakeTxnDBOptions(name);
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  Status s = DB::Open(options, &db);
  EXPECT_TRUE(s.ok()) << s.ToString();
  return db;
}

TEST(TransactionTest, ReadOwnWriteAndCommitVisible) {
  auto db = OpenTxnDB("txn_commit_visible");
  ASSERT_NE(db, nullptr);

  std::unique_ptr<Transaction> txn;
  ASSERT_TRUE(db->BeginTransaction(TxnOptions{}, &txn).ok());
  ASSERT_NE(txn, nullptr);

  ASSERT_TRUE(txn->Put("name", "alice").ok());

  std::string value;
  Status s = txn->Get("name", &value);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(value, "alice");

  ASSERT_TRUE(txn->Commit().ok());

  s = db->Get(ReadOptions{}, "name", &value);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(value, "alice");
}

TEST(TransactionTest, RollbackDropsWrites) {
  auto db = OpenTxnDB("txn_rollback");
  ASSERT_NE(db, nullptr);

  std::unique_ptr<Transaction> txn;
  ASSERT_TRUE(db->BeginTransaction(TxnOptions{}, &txn).ok());
  ASSERT_TRUE(txn->Put("k", "v").ok());
  ASSERT_TRUE(txn->Rollback().ok());

  std::string value;
  Status s = db->Get(ReadOptions{}, "k", &value);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST(TransactionTest, ConflictOnConcurrentWrite) {
  auto db = OpenTxnDB("txn_conflict");
  ASSERT_NE(db, nullptr);

  std::unique_ptr<Transaction> t1;
  std::unique_ptr<Transaction> t2;
  ASSERT_TRUE(db->BeginTransaction(TxnOptions{}, &t1).ok());
  ASSERT_TRUE(db->BeginTransaction(TxnOptions{}, &t2).ok());

  ASSERT_TRUE(t1->Put("counter", "1").ok());
  ASSERT_TRUE(t2->Put("counter", "2").ok());

  ASSERT_TRUE(t2->Commit().ok());
  Status s = t1->Commit();
  EXPECT_TRUE(s.IsAlreadyExists()) << s.ToString();

  std::string value;
  s = db->Get(ReadOptions{}, "counter", &value);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(value, "2");
}

TEST(TransactionTest, SerializableReadWriteConflict) {
  auto db = OpenTxnDB("txn_rw_conflict");
  ASSERT_NE(db, nullptr);

  ASSERT_TRUE(db->Put(WriteOptions{}, "x", "base").ok());

  std::unique_ptr<Transaction> reader_writer;
  std::unique_ptr<Transaction> writer;
  ASSERT_TRUE(db->BeginTransaction(TxnOptions{}, &reader_writer).ok());
  ASSERT_TRUE(db->BeginTransaction(TxnOptions{}, &writer).ok());

  std::string value;
  ASSERT_TRUE(reader_writer->Get("x", &value).ok());
  EXPECT_EQ(value, "base");

  ASSERT_TRUE(writer->Put("x", "new").ok());
  ASSERT_TRUE(writer->Commit().ok());

  ASSERT_TRUE(reader_writer->Put("y", "1").ok());
  Status s = reader_writer->Commit();
  EXPECT_TRUE(s.IsAlreadyExists()) << s.ToString();
}

}  // namespace
}  // namespace kv
