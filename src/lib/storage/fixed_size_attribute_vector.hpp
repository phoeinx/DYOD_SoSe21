#pragma once

#include <vector>

#include "base_attribute_vector.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
class FixedSizeAttributeVector : public BaseAttributeVector {
 public:
  explicit FixedSizeAttributeVector(size_t attribute_vector_size) {
    _attributes = std::vector<T>(attribute_vector_size);
  }
  virtual ~FixedSizeAttributeVector() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  FixedSizeAttributeVector(FixedSizeAttributeVector&&) = default;
  FixedSizeAttributeVector& operator=(FixedSizeAttributeVector&&) = default;

  // returns the value id at a given position
  ValueID get(const size_t position) const { return static_cast<ValueID>(_attributes.at(position)); }

  // sets the value id at a given position
  void set(const size_t position, const ValueID value_id) {
    DebugAssert(position >= 0 && position < _attributes.size(), "Cannot set value at invalid position");
    _attributes[position] = static_cast<T>(value_id);
  }

  // returns the number of values
  size_t size() const { return _attributes.size(); }

  // returns the width of biggest value id in bytes
  AttributeVectorWidth width() const { return static_cast<AttributeVectorWidth>(sizeof(T)); }

 private:
  std::vector<T> _attributes = {};
};
}  // namespace opossum
