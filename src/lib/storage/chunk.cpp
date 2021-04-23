#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) {
  if (size() != 0 && segment->size() != size()) {
    throw std::runtime_error("Segment needs to have same number of rows as Chunk.");
  }

  _segments.push_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(), "Needs same number of values as columns.");
  for (auto i = 0ul, end = values.size(); i < end; i++) {
    _segments[i]->append(values[i]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const { return _segments.at(column_id); }

ColumnCount Chunk::column_count() const { return ColumnCount{_segments.size()}; }

ChunkOffset Chunk::size() const {
  if (_segments.empty()) {
    return 0;
  }
  return _segments[0]->size();
}

}  // namespace opossum
