#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/storage/dictionary_segment.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  EXPECT_EQ(t.get_chunk(ChunkID{0}).size(), 0u);
  EXPECT_EQ(std::as_const(t).get_chunk(ChunkID{0}).size(), 0u);
  EXPECT_THROW(t.get_chunk(ChunkID{1}), std::exception);

  t.append({4, "Hello,"});
  t.append({6, "world"});
  EXPECT_EQ(t.get_chunk(ChunkID{0}).size(), 2u);

  t.append({3, "!"});
  EXPECT_EQ(t.get_chunk(ChunkID{1}).size(), 1u);
}

TEST_F(StorageTableTest, AddColumn) {
  t.add_column("col_3", "int");
  EXPECT_EQ(t.column_count(), 3);

  t.append({4, "Something", 5});
  EXPECT_THROW(t.add_column("col_4", "int"), std::exception);
}

TEST_F(StorageTableTest, ColumnCount) { EXPECT_EQ(t.column_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, ColumnNames) {
  std::vector<std::string> column_names = t.column_names();
  std::sort(column_names.begin(), column_names.end());
  EXPECT_EQ(column_names, std::vector<std::string>({"col_1", "col_2"}));
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.target_chunk_size(), 2u); }

TEST_F(StorageTableTest, EmplaceChunk) {
  auto int_value_segment = std::make_shared<ValueSegment<int32_t>>();
  int_value_segment->append(4);
  int_value_segment->append(6);
  int_value_segment->append(3);

  auto string_value_segment = std::make_shared<ValueSegment<std::string>>();
  string_value_segment->append("Hello,");
  string_value_segment->append("world");
  string_value_segment->append("!");

  Chunk c;

  c.add_segment(int_value_segment);

  // error because wrong number of columns
  EXPECT_THROW(t.emplace_chunk(std::move(c)), std::exception);

  Chunk c2;
  c2.add_segment(int_value_segment);
  c2.add_segment(string_value_segment);
  EXPECT_EQ(t.chunk_count(), 1u);
  t.emplace_chunk(std::move(c2));
  EXPECT_EQ(t.chunk_count(), 1u);

  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 3u);
  EXPECT_EQ(t.row_count(), 6u);

  Chunk c3;
  c3.add_segment(int_value_segment);
  c3.add_segment(string_value_segment);
  t.emplace_chunk(std::move(c3));
  EXPECT_EQ(t.chunk_count(), 4u);
  EXPECT_EQ(t.row_count(), 9u);
}

TEST_F(StorageTableTest, CompressChunk) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);

  // check whether insertion was correct
  const auto& chunk_1 = t.get_chunk(ChunkID{0});
  const auto& segment_1 = chunk_1.get_segment(ColumnID{0});
  const auto value_1 = (*segment_1)[0];
  EXPECT_EQ(type_cast<int>(value_1), 4);

  t.compress_chunk(ChunkID{0});

  // check that compression didn't change anything
  const auto& chunk_2 = t.get_chunk(ChunkID{0});
  const auto& segment_2 = chunk_2.get_segment(ColumnID{0});
  const auto value_2 = (*segment_2)[0];
  EXPECT_EQ(type_cast<int>(value_2), 4);

  // check that the chunk is now a DictionarySegment
  const auto& chunk_3 = t.get_chunk(ChunkID{0});
  const auto& segment_3 = chunk_3.get_segment(ColumnID{0});
  EXPECT_NE(std::dynamic_pointer_cast<DictionarySegment<int>>(segment_3), nullptr);

}

}  // namespace opossum
