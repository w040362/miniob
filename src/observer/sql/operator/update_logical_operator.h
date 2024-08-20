#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/value.h"
#include "storage/field/field.h"

/**
 * @brief 逻辑算子，用于执行delete语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, std::vector<Value*> &values, std::vector<FieldMeta*> &fields);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  Table              *table() const { return table_; }

  std::vector<Value*> &values() { return values_; }
  const std::vector<Value*> &values() const { return values_; }
  
  std::vector<FieldMeta*> &fields() { return fields_; };
  const std::vector<FieldMeta*> &fields() const { return fields_; };

private:
  Table *table_ = nullptr;
  std::vector<Value*> values_;
  std::vector<FieldMeta*> fields_;
};