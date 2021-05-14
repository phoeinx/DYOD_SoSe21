#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value) : AbstractOperator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

  ColumnID column_id() const { return _column_id; }
  ScanType scan_type() const { return _scan_type; }
  const AllTypeVariant& search_value() const { return _search_value; }

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    auto input_table = _left_input_table();
    std::vector<RowID> position_list;
    const auto num_chunks = input_table->chunk_count();
    for (auto chunk_id = 0u; chunk_id < num_chunks; ++chunk_id){
      auto segment = input_table->get_chunk(static_cast<ChunkID>(chunk_id)).get_segment(_column_id);
    }
    // Auf der Tablle mit der ColumnId
    // safe row ids für die die bedingung stimmt
    // beim zurückgeben bauen wir eine tabelle mit reference segments
  };
  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;


};

}  // namespace opossum
