#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class BenchmarkTableScanTest : public BaseTest {
protected:
 void SetUp() override {

   std::shared_ptr<Table> test_dict = std::make_shared<Table>(5);
   test_dict->add_column("FirstId", "int");
   test_dict->add_column("SecondId", "int");
   test_dict->add_column("FirstName", "string");
   test_dict->add_column("LastName", "string");
   for (int i = 0; i <= 2400000; i += 1) test_dict->append({i, 1000 + i, "TestFirst", "TestLast"});

   test_dict->compress_chunk(ChunkID(0));
   test_dict->compress_chunk(ChunkID(2));

   _table_wrapper = std::make_shared<TableWrapper>(std::move(test_dict));
   _table_wrapper->execute();
 }

 std::shared_ptr<TableWrapper> _table_wrapper;
};

TEST_F(BenchmarkTableScanTest, DoubleScan) {

 auto scan_1 = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);

}

}  // namespace opossum
