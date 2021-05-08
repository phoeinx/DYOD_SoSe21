#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
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
  Assert(row_count() == 0, "Cannot add column to non-empty table");

  _column_names.push_back(name);
  _column_types.push_back(type);
  _add_value_segment_to_chunk(_chunks[0], type);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() == _target_chunk_size) {
    auto new_chunk = std::make_shared<Chunk>();
    for (auto column_type : _column_types) {
      _add_value_segment_to_chunk(new_chunk, column_type);
    }
    _chunks.push_back(new_chunk);
  }

  _chunks.back()->append(values);
}

ColumnCount Table::column_count() const { return _chunks[0]->column_count(); }

uint64_t Table::row_count() const {
  uint64_t row_count = 0;
  for (const auto &chunk : _chunks) {
    row_count = row_count + chunk->size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto it = std::find(_column_names.begin(), _column_names.end(), column_name);
  Assert(it != _column_names.end(), "Column not found");

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

void Table::_add_dictionary_segment_to_chunk(std::shared_ptr<Chunk>& chunk, const std::string& type,
                                             const std::shared_ptr<BaseSegment>& value_segment) {
  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto dictionary_segment = std::make_shared<DictionarySegment<ColumnDataType>>(value_segment);
    chunk->add_segment(dictionary_segment);
  });
}

void Table::compress_chunk(ChunkID chunk_id) {
  auto compressed_chunk = std::make_shared<Chunk>();
  const auto& chunk = get_chunk(chunk_id);
  const auto chunk_column_count = chunk.column_count();

  std::vector<std::thread> threads;

  for (auto column = 0; column < chunk_column_count; column++) {
     const auto segment_type = _column_types[column];
     auto segment = chunk.get_segment(static_cast<ColumnID>(column));

    threads.emplace_back(std::thread(&Table::_add_dictionary_segment_to_chunk, this, std::ref(compressed_chunk), segment_type, segment));
  }
  for (size_t index = 0; index < threads.size(); index++) {
    threads[index].join();
  }
  _chunks[chunk_id] = compressed_chunk;
}

}  // namespace opossum
