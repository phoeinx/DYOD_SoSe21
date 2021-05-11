#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "fixed_size_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseSegment;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    const auto segment_size = base_segment->size();
    _dictionary = std::make_shared<std::vector<T>>(segment_size);

    for (auto cell_id = 0u; cell_id < segment_size; ++cell_id) {
      (*_dictionary)[cell_id] = type_cast<T>((*base_segment)[cell_id]);
    }

    std::sort(_dictionary->begin(), _dictionary->end());
    const auto first_duplicate_element = std::unique(_dictionary->begin(), _dictionary->end());
    _dictionary->erase(first_duplicate_element, _dictionary->end());
    _dictionary->shrink_to_fit();

    const auto dictionary_size = _dictionary->size();

    // Calculate appropriate width for attribute vector
    // by finding the number of bits needed to store the maximum valueID
    const auto required_bit_size = std::ceil(std::log(dictionary_size) / std::log(2));

    if (required_bit_size <= 8) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint8_t>>(segment_size);
    } else if (required_bit_size <= 16) {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint16_t>>(segment_size);
    } else {
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint32_t>>(segment_size);
    }

    for (auto cell_id = 0u; cell_id < segment_size; ++cell_id) {
      const auto value = type_cast<T>((*base_segment)[cell_id]);
      const auto dictionary_offset =
          std::distance(_dictionary->cbegin(), std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value));
      (*_attribute_vector).set(cell_id, static_cast<ValueID>(dictionary_offset));
    }
  }

  // returns the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override {
    const auto dictionary_value = _dictionary->at(_attribute_vector->get(chunk_offset));
    return static_cast<AllTypeVariant>(dictionary_value);
  }

  // returns the value at a certain position.
  T get(const size_t chunk_offset) const { return _dictionary->at(_attribute_vector->get(chunk_offset)); }

  // dictionary segments are immutable
  void append(const AllTypeVariant& val) override { throw std::runtime_error("Cannot append to immutable segments"); }

  // returns the underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns the attribute vector
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // returns the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    const auto lower_bound_it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (lower_bound_it == _dictionary->end()) {
      return INVALID_VALUE_ID;
    }
    return static_cast<ValueID>(std::distance(_dictionary->cbegin(), lower_bound_it));
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const {
    const T typed_value = static_cast<T>(value);
    return lower_bound(typed_value);
  }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    const auto upper_bound_it = std::upper_bound(_dictionary->begin(), _dictionary->end(), value);
    if (upper_bound_it == _dictionary->end()) {
      return INVALID_VALUE_ID;
    }
    return static_cast<ValueID>(std::distance(_dictionary->begin(), upper_bound_it));
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const {
    const T typed_value = static_cast<T>(value);
    return upper_bound(typed_value);
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  ChunkOffset size() const override { return static_cast<ChunkOffset>(_attribute_vector->size()); }

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final {
    return (sizeof(T) * _dictionary->size() + _attribute_vector->width() * _attribute_vector->size());
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
