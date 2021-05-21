#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager storageManager;
  return storageManager;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(!has_table(name), "Cannot add table, because a table with this name already exists");
  _tables[name] = table;
}

void StorageManager::drop_table(const std::string& name) {
  Assert(has_table(name), "Cannot drop non-existing table");
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  table_names.reserve(_tables.size());
  for (const auto& [table_name, table] : _tables) {
    table_names.push_back(table_name);
  }
  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  std::cout << "StorageManager #tables: " << _tables.size() << std::endl;
  for (const auto& [table_name, table] : _tables) {
    std::cout << table_name << " #columns: " << table->column_count() << " #rows: " << table->row_count()
              << " #chunks: " << table->chunk_count() << std::endl;
  }
}

void StorageManager::reset() { get() = StorageManager{}; }

}  // namespace opossum
