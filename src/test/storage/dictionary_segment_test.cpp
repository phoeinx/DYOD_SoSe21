#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_segment.hpp"
#include "../../lib/storage/dictionary_segment.hpp"
#include "../../lib/storage/value_segment.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<opossum::ValueSegment<int>> vc_int = std::make_shared<opossum::ValueSegment<int>>();
  std::shared_ptr<opossum::ValueSegment<std::string>> vc_str = std::make_shared<opossum::ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressStringSegment) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_str);
  });
  const auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (number of unique values)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  const auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");

  // Test value access
  EXPECT_EQ(dict_col->get(0), "Bill");
  EXPECT_EQ(dict_col->get(5), "Bill");
  EXPECT_THROW(dict_col->get(6), std::exception);

  EXPECT_EQ((*dict_col)[0], static_cast<AllTypeVariant>("Bill"));
  EXPECT_EQ((*dict_col)[5], static_cast<AllTypeVariant>("Bill"));
  EXPECT_THROW((*dict_col)[6], std::exception);
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  const auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (opossum::ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (opossum::ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), opossum::INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), opossum::INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, UseFixedSizeAttributeVector) {
  for (int i = 0; i < 200; i += 1) vc_int->append(i);

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  const auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  const auto attribute_vector = dict_col->attribute_vector();

  EXPECT_EQ(attribute_vector->width(), 1);

  // adding 100 more entries means that a uint16_t is necessary
  for (int i = 200; i < 301; i += 1) vc_int->append(i);

  std::shared_ptr<BaseSegment> col_2;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col_2 = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  const auto dict_col_2 = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col_2);

  const auto attribute_vector_2 = dict_col_2->attribute_vector();

  EXPECT_EQ(attribute_vector_2->width(), 2);

  // adding 66_000 more entries means that a uint32_t is necessary
  for (int i = 301; i < 66000; i += 1) vc_int->append(i);

  std::shared_ptr<BaseSegment> col_3;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col_3 = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  const auto dict_col_3 = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col_3);

  const auto attribute_vector_3 = dict_col_3->attribute_vector();

  EXPECT_EQ(attribute_vector_3->width(), 4);
}

TEST_F(StorageDictionarySegmentTest, MemoryEstimation) {
  for (int i = 0; i < 300; i += 1) vc_int->append(i);

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  const auto dict_col_int = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  // 300 elements with 4 bytes (byte size of an int) per element for the value segment
  EXPECT_EQ(vc_int->estimate_memory_usage(), 1200);

  // 300 elements * 4 byte per int from the dictionary + 300 elements * 2 byte (uint16_t) from the attribute vector
  EXPECT_EQ(dict_col_int->estimate_memory_usage(), 1800);

  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  std::shared_ptr<BaseSegment> col_2;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    col_2 = std::make_shared<DictionarySegment<Type>>(vc_str);
  });
  const auto dict_col_str = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col_2);

  // should be smaller, since we reduced number of strings we need to store
  EXPECT_EQ(vc_str->estimate_memory_usage() > dict_col_str->estimate_memory_usage(), true);
}

TEST_F(StorageDictionarySegmentTest, AppendRow) {
  for (int i = 0; i < 300; i += 1) vc_int->append(i);

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  const auto dict_col_int = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  const auto value = static_cast<AllTypeVariant>(0);
  EXPECT_THROW(dict_col_int->append(value), std::exception);
}

TEST_F(StorageDictionarySegmentTest, GetValueByValueId) {
  for (int i = 0; i < 10; i += 1) vc_int->append(i);

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  const auto dict_col_int = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col_int->value_by_value_id(static_cast<ValueID>(5)), 5);
  EXPECT_NE(dict_col_int->value_by_value_id(static_cast<ValueID>(7)), 8);
  EXPECT_THROW(dict_col_int->value_by_value_id(static_cast<ValueID>(10)), std::exception);
}

}  // namespace opossum
