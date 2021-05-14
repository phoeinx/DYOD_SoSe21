#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class GetTable : public AbstractOperator {
 public:
  explicit GetTable(const std::string& name) : _table_name(name) {}

  const std::string& table_name() const {
    return _table_name; 
  }

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    return StorageManager::get().get_table(_table_name);
  }
  const std::string _table_name;
};
}  // namespace opossum
 