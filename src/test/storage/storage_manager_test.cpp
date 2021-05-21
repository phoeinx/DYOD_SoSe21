#include <iostream>
#include <memory>
#include <sstream>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageStorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto t1 = std::make_shared<Table>(1);
    auto t2 = std::make_shared<Table>(4);

    sm.add_table("first_table", t1);
    sm.add_table("second_table", t2);
  }
};

TEST_F(StorageStorageManagerTest, AddTable) {
  auto& sm = StorageManager::get();
  auto t3 = std::make_shared<Table>();

  sm.add_table("third_table", t3);
  EXPECT_EQ(sm.table_names().size(), 3u);

  EXPECT_THROW(sm.add_table("first_table", t3), std::exception);
}

TEST_F(StorageStorageManagerTest, GetTable) {
  auto& sm = StorageManager::get();
  auto t1 = sm.get_table("first_table");
  EXPECT_EQ(t1->target_chunk_size(), 1u);

  auto t2 = sm.get_table("second_table");
  EXPECT_EQ(t2->target_chunk_size(), 4u);

  EXPECT_THROW(sm.get_table("third_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DropTable) {
  auto& sm = StorageManager::get();
  sm.drop_table("first_table");
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
  EXPECT_THROW(sm.drop_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, ResetTable) {
  StorageManager::get().reset();
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DoesNotHaveTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("third_table"), false);
}

TEST_F(StorageStorageManagerTest, HasTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("first_table"), true);
}

TEST_F(StorageStorageManagerTest, GetTableNames) {
  std::vector<std::string> sm_table_names = StorageManager::get().table_names();

  std::sort(sm_table_names.begin(), sm_table_names.end());

  EXPECT_EQ(sm_table_names, std::vector<std::string>({"first_table", "second_table"}));
}

TEST_F(StorageStorageManagerTest, PrintTable) {
  auto& sm = StorageManager::get();
  sm.drop_table("second_table");
  const std::string expected =
      "StorageManager #tables: 1\n"
      "first_table #columns: 0 #rows: 0 #chunks: 1\n";

  std::stringstream buffer;
  std::streambuf* prevcoutbuf = std::cout.rdbuf(buffer.rdbuf());

  sm.print();
  std::string printed_text = buffer.str();
  // Restore original buffer before exiting
  std::cout.rdbuf(prevcoutbuf);

  EXPECT_EQ(printed_text, expected);
}

}  // namespace opossum
