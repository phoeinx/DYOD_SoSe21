#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <resolve_type.hpp>
#include <storage/reference_segment.hpp>
#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Operator that filters a column of the table by comparing all values to a given search value with a given operator.
 * It returns a table with exactly one ReferenceSegment for each column that references the resulting rows.
 */

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  // returns a fitting comparator function for the ScanType for two equally typed values
  // the type must be provided via the template parameter
  template <typename T>
  std::function<bool(T, T)> _get_comparator(ScanType type);

  // returns the correct ValueID (based on the ScanType) to compare against
  // when using index-based comparison in DictionarySegements
  ValueID _get_compare_value_id(ScanType type, ValueID upper_bound, ValueID lower_bound);

  // returns a list of RowIDs referencing all rows in the table for which the scan evaluated to true
  std::vector<RowID> _create_position_list(const std::shared_ptr<const Table>& input_table);

  // returns a table with one reference segment per column
  // which references the rows for which the scan evaluated to true
  std::shared_ptr<const Table> _create_reference_output_table(const std::shared_ptr<const Table>& input_table,
                                                              const std::vector<RowID>& position_list,
                                                              ChunkOffset target_chunk_size, ColumnCount column_count);

  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;
};

}  // namespace opossum
