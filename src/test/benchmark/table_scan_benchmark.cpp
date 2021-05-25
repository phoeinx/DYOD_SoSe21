#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <chrono>

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

   // Setup large test table
   std::shared_ptr<Table> large_test_table = std::make_shared<Table>(5);
   large_test_table->add_column("FirstId", "int");
   large_test_table->add_column("SecondId", "int");
   large_test_table->add_column("FirstName", "string");
   large_test_table->add_column("LastName", "string");
   for (int i = 0; i <= 2400000; i += 1) large_test_table->append({i, 1000 + i, "TestFirst", "TestLast"});

   large_test_table->compress_chunk(ChunkID(0));
   large_test_table->compress_chunk(ChunkID(2));

   _large_table_wrapper = std::make_shared<TableWrapper>(std::move(large_test_table));
   _large_table_wrapper->execute();

  // Setup medium test table
   std::shared_ptr<Table> medium_test_table = std::make_shared<Table>(5);
   medium_test_table->add_column("FirstId", "int");
   medium_test_table->add_column("SecondId", "int");
   medium_test_table->add_column("FirstName", "string");
   for (int i = 0; i <= 24000; i += 1) medium_test_table->append({i, 1000 + i, "TestFirst"});

   medium_test_table->compress_chunk(ChunkID(0));

   _medium_table_wrapper = std::make_shared<TableWrapper>(std::move(medium_test_table));
   _medium_table_wrapper->execute();

    // Setup small test table
   std::shared_ptr<Table> small_test_table = std::make_shared<Table>(5);
   small_test_table->add_column("FirstId", "int");
   small_test_table->add_column("FirstName", "string");
   for (int i = 0; i <= 240; i += 1) small_test_table->append({i,"TestFirst"});

   _small_table_wrapper = std::make_shared<TableWrapper>(std::move(small_test_table));
   _small_table_wrapper->execute();

   using namespace std::chrono;
   start_ms = duration_cast< nanoseconds >(system_clock::now().time_since_epoch());
 }

 void TearDown() override {
    // Gets the time when the test finishes
  using namespace std::chrono;
  nanoseconds end_ms = duration_cast< nanoseconds >(system_clock::now().time_since_epoch());
  std::cout << " Starttime: " << std::to_string(start_ms.count()) << " - " << " Endtime: " << std::to_string(end_ms.count()) << " - " <<  " Time consumed: " << std::to_string(end_ms.count() - start_ms.count()) << std::endl;
  }

 std::shared_ptr<TableWrapper> _large_table_wrapper, _medium_table_wrapper, _small_table_wrapper;
 std::chrono::nanoseconds start_ms;
};

TEST_F(BenchmarkTableScanTest, SingleScanOnLargeTable) {

 auto scan_1 = std::make_shared<TableScan>(_large_table_wrapper, ColumnID{0}, ScanType::OpEquals, 12340);
}

TEST_F(BenchmarkTableScanTest, SingleScanOnMediumTable) {

 auto scan_1 = std::make_shared<TableScan>(_medium_table_wrapper, ColumnID{0}, ScanType::OpLessThanEquals, 12000);
}

TEST_F(BenchmarkTableScanTest, SingleScanOnSmallTable) {

 auto scan_1 = std::make_shared<TableScan>(_small_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 60);
}

TEST_F(BenchmarkTableScanTest, TripleScanOnSmallTable) {

 auto scan_1 = std::make_shared<TableScan>(_small_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 60);
 auto scan_2 = std::make_shared<TableScan>(scan_1, ColumnID{0}, ScanType::OpLessThan, 180);
 auto scan_3 = std::make_shared<TableScan>(scan_2, ColumnID{1}, ScanType::OpEquals, "TestFirst");

}

TEST_F(BenchmarkTableScanTest, TripleScanOnMediumTable) {

 auto scan_1 = std::make_shared<TableScan>(_medium_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 6000);
 auto scan_2 = std::make_shared<TableScan>(scan_1, ColumnID{0}, ScanType::OpLessThan, 18000);
 auto scan_3 = std::make_shared<TableScan>(scan_2, ColumnID{1}, ScanType::OpEquals, "TestFirst");

}

TEST_F(BenchmarkTableScanTest, TripleScanOnLargeTable) {

 auto scan_1 = std::make_shared<TableScan>(_large_table_wrapper, ColumnID{0}, ScanType::OpGreaterThanEquals, 600000);
 auto scan_2 = std::make_shared<TableScan>(scan_1, ColumnID{0}, ScanType::OpLessThan, 1800000);
 auto scan_3 = std::make_shared<TableScan>(scan_2, ColumnID{2}, ScanType::OpEquals, "TestFirst");

}


}  // namespace opossum
