#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) {
  // Implementation goes here
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::add_column(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  // Implementation goes here
}

void Table::create_new_chunk() {
  // Implementation goes here
}

ColumnCount Table::column_count() const {
  // Implementation goes here
  return ColumnCount{0};
}

uint64_t Table::row_count() const {
  // Implementation goes here
  return 0;
}

ChunkID Table::chunk_count() const {
  // Implementation goes here
  return ChunkID{0};
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  // Implementation goes here
  return ColumnID{0};
}

ChunkOffset Table::target_chunk_size() const {
  // Implementation goes here
  return 0;
}

const std::vector<std::string>& Table::column_names() const {
  throw std::runtime_error("Implement Table::column_names()");
}

const std::string& Table::column_name(const ColumnID column_id) const {
  throw std::runtime_error("Implement Table::column_name");
}

const std::string& Table::column_type(const ColumnID column_id) const {
  throw std::runtime_error("Implement Table::column_type");
}

Chunk& Table::get_chunk(ChunkID chunk_id) { throw std::runtime_error("Implement Table::get_chunk"); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { throw std::runtime_error("Implement Table::get_chunk"); }

void Table::compress_chunk(ChunkID chunk_id) { throw std::runtime_error("Implement Table::compress_chunk"); }

}  // namespace opossum
