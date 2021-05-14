#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "dictionary_segment.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

// ReferenceSegment is a specific segment type that stores all its values as position list of a referenced segment
class ReferenceSegment : public BaseSegment {
 public:
  // creates a reference segment
  // the parameters specify the positions and the referenced segment
  ReferenceSegment(const std::shared_ptr<const Table>& referenced_table, const ColumnID referenced_column_id,
                   const std::shared_ptr<const PosList>& pos) : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id),
                    _position_list(pos){};

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override {
    auto position = _position_list->at(chunk_offset);

    // TODO:Refactoring neccessary
    return (*(_referenced_table->get_chunk(position.chunk_id)).get_segment(_referenced_column_id))[position.chunk_offset];
  };

  void append(const AllTypeVariant&) override { throw std::logic_error("ReferenceSegment is immutable"); };

  ChunkOffset size() const override {
      return static_cast<ChunkOffset>(_position_list->size());
  }

  const std::shared_ptr<const PosList>& pos_list() const {
      return _position_list;
  }
  const std::shared_ptr<const Table>& referenced_table() const {
      return _referenced_table;
  }

  ColumnID referenced_column_id() const {
      return _referenced_column_id;
  }

  size_t estimate_memory_usage() const final {
    return (sizeof(RowID) * _position_list->size());
  }

 protected:
  const std::shared_ptr<const Table> _referenced_table;
  const ColumnID _referenced_column_id;
  const std::shared_ptr<const PosList> _position_list;
};

}  // namespace opossum
