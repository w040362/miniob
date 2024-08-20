#pragma once

#include <vector>

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"

class Trx;
class UpdateStmt;

/**
 * @brief 物理算子，更新
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, std::vector<Value*> &values, std::vector<FieldMeta*> &fields) : table_(table), values_(values), fields_(fields)
  {}

  virtual ~UpdatePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  std::vector<Value*> &values() { return values_; }
  const std::vector<Value*> &values() const { return values_; }
  
  std::vector<FieldMeta*> &fields() { return fields_; };
  const std::vector<FieldMeta*> &fields() const { return fields_; };

  Tuple *current_tuple() override
  {
    return nullptr;
  }

private:
  Table *table_ = nullptr;
  Trx   *trx_ = nullptr;
  std::vector<Value*> values_;
  std::vector<FieldMeta*> fields_;
  std::vector<Record> records_;
};