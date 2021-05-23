#include "table_scan.hpp"

namespace opossum {

const int64_t EMPTY_RESULT{-2};

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator{in}, _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  auto input_table = _left_input_table();

  //TODO: Check if _scan_type is the same type as the column to be searched

  std::vector<RowID> position_list = _create_position_list(input_table);

  // Check if the input table consists of reference segments or not
  auto first_input_segment = input_table->get_chunk(ChunkID{0}).get_segment(ColumnID{0});
  const auto typed_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(first_input_segment);
  if ( typed_reference_segment != nullptr )  {
    input_table = typed_reference_segment->referenced_table();
  }
  return _create_reference_output_table(input_table, std::move(position_list), input_table->target_chunk_size(), input_table->column_count());
}

template <typename T>
std::function<bool(T, T)> TableScan::_get_comparator(ScanType type) {
  std::function<bool(T, T)> _return;

  switch (type) {
    case ScanType::OpEquals: {
      _return = [](auto left, auto right) { return left == right; };
      break;
    }
    case ScanType::OpNotEquals: {
      _return = [](auto left, auto right) { return left != right; };
      break;
    }
    case ScanType::OpGreaterThanEquals: {
      _return = [](auto left, auto right) { return left >= right; };
      break;
    }
    case ScanType::OpGreaterThan: {
      _return = [](auto left, auto right) { return left > right; };
      break;
    }
    case ScanType::OpLessThanEquals: {
      _return = [](auto left, auto right) { return left <= right; };
      break;
    }
    case ScanType::OpLessThan: {
      _return = [](auto left, auto right) { return left < right; };
      break;
    }
    default: {
      throw std::runtime_error("Operator not implemented");
      break;
    }
  }
  return _return;
}

// TODO: is it possible w/ smaller value type?
int64_t TableScan::_get_compare_value(ScanType type, ValueID upper_bound, ValueID lower_bound) {

  switch (type) {
    case ScanType::OpEquals: {
      // when not in dictionary, select nothing
      if (upper_bound != lower_bound) {
        return lower_bound;
      } else {
        return EMPTY_RESULT;
      }
      break;
    }
    case ScanType::OpNotEquals: {
      // when not in dictionary, select all
      return upper_bound != lower_bound ? lower_bound : INVALID_VALUE_ID;
      break;
    }
    case ScanType::OpGreaterThanEquals: {
      // when lower_bound == 0, select everything
      // when lower_bound == INVALID_VALUE_ID, select nothing
      if (lower_bound == INVALID_VALUE_ID) {
        return EMPTY_RESULT;
      }
      return lower_bound;
      break;
    }
    case ScanType::OpGreaterThan: {
      // if the search value exists in our dictionary we can use it as the value to compare against with greater than 
      // if the search value does not exist in our dictionary, we have to take the next smaller value to compare against
      // subtracting one is safe, because in the case that our search value is smaller than every value in the dictionary
      // the resulting compare value becomes negative and every ValueID of the dictionary is greater than a negative number

      // when upper_bound == lower_bound && upper_bound == 0, select everything
      // when upper_bound == lower_bound && upper_bound == INVALID_VALUE_ID, select nothing
      if (upper_bound == lower_bound && upper_bound == ValueID{0}) { 
        return -1;
      } else if (upper_bound == lower_bound && upper_bound == INVALID_VALUE_ID) {
        return EMPTY_RESULT;
      } else {
        return upper_bound != lower_bound ? lower_bound : ValueID{lower_bound - 1}; 
      }
      break;
    }
    case ScanType::OpLessThanEquals: {
      // when upper_bound == lower_bound && upper_bound == 0, select nothing
      // when upper_bound == lower_bound && upper_bound == INVALID_VALUE_ID, select everything
      if (upper_bound == lower_bound && upper_bound == ValueID{0} ) { 
        return EMPTY_RESULT;
      } else if (upper_bound == lower_bound && upper_bound == INVALID_VALUE_ID) {
        return INVALID_VALUE_ID;
      } else {
        return upper_bound != lower_bound ? lower_bound : ValueID{lower_bound - 1};
      }
      break;
    }
    case ScanType::OpLessThan: {
      // when upper_bound == lower_bound && upper_bound == 0, select nothing
      // when upper_bound == lower_bound && upper_bound == INVALID_VALUE_ID, select everything
      if (upper_bound == lower_bound && upper_bound == ValueID{0} ) { 
        return EMPTY_RESULT;
      } else {
        return lower_bound;
      }
      break;
    }
    default: {
      throw std::runtime_error("Operator not implemented");
      break;
    }
  }
  return INVALID_VALUE_ID;
}

std::vector<RowID> TableScan::_create_position_list(const std::shared_ptr<const Table>& input_table) {
  const auto num_chunks = input_table->chunk_count();
  std::vector<RowID> position_list;
  
  resolve_data_type(input_table->column_type(_column_id), [&](auto type) {
    using Type = typename decltype(type)::type;
    Type typed_search_value = type_cast<Type>(_search_value);

    // Search in each chunk in column
    for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; ++chunk_id) {
      auto segment = input_table->get_chunk(chunk_id).get_segment(_column_id);

      // Check for segment type
      const auto typed_value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
      if (typed_value_segment != nullptr) {
        std::vector<Type> values = typed_value_segment->values();
        auto comparator = _get_comparator<Type>(_scan_type);

        // Compare each value of value segment against the compare value with comparator
        for (auto cell_id = 0ul, typed_segment_size = values.size(); cell_id < typed_segment_size; ++cell_id) {
          if (comparator(values[cell_id], typed_search_value)) {
            position_list.push_back(RowID{static_cast<ChunkID>(chunk_id), static_cast<ChunkOffset>(cell_id)});
          }
        }
      } else if (const auto typed_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment); typed_dictionary_segment != nullptr ) {
          auto dictionary_comparator = _get_comparator<int64_t>(_scan_type);
          //const auto typed_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
          auto attribute_value_ids = typed_dictionary_segment->attribute_vector();

          auto search_value_id = _get_compare_value(_scan_type, typed_dictionary_segment->upper_bound(typed_search_value), typed_dictionary_segment->lower_bound(typed_search_value));

          if (search_value_id == EMPTY_RESULT) {
            continue;
          }

          //Compare each value of dictionary segment against the compare value with comparator
          for (auto cell_id = 0ul, typed_segment_size = attribute_value_ids->size(); cell_id < typed_segment_size; ++cell_id) {
            if (dictionary_comparator(attribute_value_ids->get(cell_id), search_value_id)) {
              position_list.push_back(RowID{static_cast<ChunkID>(chunk_id), static_cast<ChunkOffset>(cell_id)});
            }
          }
      } else if (const auto typed_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment); typed_reference_segment != nullptr ) {
        //TODO: Is more performance possible
        auto dictionary_comparator = _get_comparator<Type>(_scan_type);

        auto input_position_list = typed_reference_segment->pos_list();
        auto input_reference_table = typed_reference_segment->referenced_table();
        // Compare each value of dictionary segment against the compare value with comparator
        for (const auto& row_id : *input_position_list) {
          const auto dereferenced_value = type_cast<Type>((*(input_reference_table->get_chunk(row_id.chunk_id).get_segment(_column_id)))[row_id.chunk_offset]);
          if (dictionary_comparator(dereferenced_value, typed_search_value)) {
            position_list.push_back(row_id);
          }
        }
      }
    }
  });
  return position_list;
}

std::shared_ptr<const Table> TableScan::_create_reference_output_table(const std::shared_ptr<const Table>& input_table, const std::vector<RowID>& position_list, ChunkOffset target_chunk_size, ColumnCount column_count) {
  Chunk chunk;
  auto ref_position_list = std::make_shared<PosList>(std::move(position_list));
  auto resulting_table = std::make_shared<Table>(target_chunk_size);

  for (auto current_column_id = static_cast<ColumnCount>(0), columns = input_table->column_count();
       current_column_id < columns; current_column_id++) {
    chunk.add_segment(
        std::make_shared<ReferenceSegment>(input_table, static_cast<ColumnID>(current_column_id), ref_position_list));
    resulting_table->add_column(input_table->column_name(static_cast<ColumnID>(current_column_id)),
                                input_table->column_type(static_cast<ColumnID>(current_column_id)));
  }
  resulting_table->emplace_chunk(std::move(chunk));
  return resulting_table;
}


}  // namespace opossum
