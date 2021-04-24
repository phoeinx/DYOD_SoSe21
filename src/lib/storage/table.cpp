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
  _target_chunk_size = target_chunk_size;
  _chunks.push_back(std::make_shared<Chunk>());
}

void Table::add_column(const std::string& name, const std::string& type) {
  if (row_count() != 0) {
    throw std::runtime_error("Cannot add column to non-empty table");
  }
  _column_names.push_back(name);
  _column_types.push_back(type);
  _add_value_segment_to_chunk(_chunks[0], type);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  auto last_chunk = _chunks[chunk_count() - 1];
  if (last_chunk->size() == _target_chunk_size) {
    last_chunk = std::make_shared<Chunk>();
    for (auto column_type : _column_types) {
      _add_value_segment_to_chunk(last_chunk, column_type);
    }
    _chunks.push_back(last_chunk);
  }
  last_chunk->append(values);
}

ColumnCount Table::column_count() const { return _chunks[0]->column_count(); }

uint64_t Table::row_count() const {
  const auto completed_chunks_size = (chunk_count() - 1) * _target_chunk_size;
  const auto last_chunk_size = _chunks[chunk_count() - 1]->size();
  return completed_chunks_size + last_chunk_size;
}

ChunkID Table::chunk_count() const { return ChunkID{_chunks.size()}; }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto it = std::find(_column_names.begin(), _column_names.end(), column_name);
  if (it == _column_names.end()) {
    throw std::runtime_error("Column not found");
  }
  return static_cast<ColumnID>(std::distance(_column_names.begin(), it));
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) { return *_chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return *_chunks.at(chunk_id); }

void Table::_add_value_segment_to_chunk(std::shared_ptr<Chunk>& chunk, const std::string& type) {
  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
    chunk->add_segment(value_segment);
  });
}

}  // namespace opossum
