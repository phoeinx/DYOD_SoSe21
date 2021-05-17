#include "table_scan.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator{in}, _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  auto input_table = _left_input_table();

  std::vector<RowID> position_list = _create_position_list(input_table);
  // TODO: Create reference segment with position list
  // TODO: Resolve possible indirections over reference segments in input_table
  return _create_reference_output_table(input_table, std::move(position_list), input_table->target_chunk_size(), input_table->column_count());
};

template <typename T>
std::function<bool(T, T)> TableScan::_get_comparator(ScanType type) {
  std::function<bool(T, T)> _return = [](auto left, auto right) { return left == right; };

  switch (type) {
    case ScanType::OpEquals: {
      _return = [](auto left, auto right) { return left == right; };
    }
    case ScanType::OpNotEquals: {
      _return = [](auto left, auto right) { return left != right; };
    }
    default: {
      _return = [](auto left, auto right) { return left != right; };
    }
  }
  return _return;
}

std::vector<RowID> TableScan::_create_position_list(const std::shared_ptr<const Table>& input_table) {
  const auto num_chunks = input_table->chunk_count();
  std::vector<RowID> position_list;
  
  resolve_data_type(input_table->column_type(_column_id), [&](auto type) {
    using Type = typename decltype(type)::type;
    Type typed_search_value = type_cast<Type>(_search_value);

    for (auto chunk_id = 0u; chunk_id < num_chunks; ++chunk_id) {
      auto segment = input_table->get_chunk(static_cast<ChunkID>(chunk_id)).get_segment(_column_id);

      const auto typed_value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
      if (typed_value_segment != nullptr) {
        std::vector<Type> values = typed_value_segment->values();
        auto comparator = _get_comparator<Type>(_scan_type);

        for (auto cell_id = 0ul, typed_segment_size = values.size(); cell_id < typed_segment_size; ++cell_id) {
          if (comparator(values[cell_id], typed_search_value)) {
            position_list.push_back(RowID{static_cast<ChunkID>(chunk_id), static_cast<ChunkOffset>(cell_id)});
          }
        }
      } else {
        auto dictionary_comparator = _get_comparator<int>(_scan_type);
        const auto typed_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
        auto values = typed_dictionary_segment->attribute_vector();
        auto search_value_id = typed_dictionary_segment->lower_bound(typed_search_value);
        for (auto cell_id = 0ul, typed_segment_size = values->size(); cell_id < typed_segment_size; ++cell_id) {
          if (dictionary_comparator(values->get(cell_id), search_value_id)) {
            position_list.push_back(RowID{static_cast<ChunkID>(chunk_id), static_cast<ChunkOffset>(cell_id)});
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
