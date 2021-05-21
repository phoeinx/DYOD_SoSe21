#pragma once

#include <memory>
#include <optional>
#include <resolve_type.hpp>
#include <storage/reference_segment.hpp>
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
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value);

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template <typename T>
  std::function<bool(T, T)> _get_comparator(ScanType type);

  std::shared_ptr<const Table> _create_reference_output_table(const std::shared_ptr<const Table>& input_table, const std::vector<RowID>& position_list, ChunkOffset target_chunk_size, ColumnCount column_count);
  std::vector<RowID> _create_position_list(const std::shared_ptr<const Table>& input_table);
  int64_t _get_compare_value(ScanType type, ValueID upper_bound, ValueID lower_bound);

  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;
};

}  // namespace opossum
