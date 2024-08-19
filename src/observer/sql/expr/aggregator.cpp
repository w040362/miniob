/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  if (value_.attr_type() == AttrType::NULLS) {
    // 第一行合并的是 null
    value_ = value;
  } else {
    if (value.attr_type() == AttrType::NULLS) {
      // null: +0
      return RC::SUCCESS;
    }

    ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
          attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
    
    switch (value.attr_type())
    {
      case AttrType::INTS: {
        value_.set_int(value.get_int() + value_.get_int());
      } break;
      case AttrType::FLOATS: {
        value_.set_float(value.get_float() + value_.get_float());
      } break;
      default: {
        return RC::INTERNAL;
      }
    }
  }
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value& value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (value_.attr_type() == AttrType::NULLS) {
    // 第一行合并的是 null
    value_ = value;
  } else {
    if (value.attr_type() == AttrType::NULLS) {
      // null: +0
      return RC::SUCCESS;
    }

    ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
          attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

    int result = value_.compare(value);
    if (result < 0) {
      value_ = value;
    }
  }
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value& value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (value_.attr_type() == AttrType::NULLS) {
    // 第一行合并的是 null
    value_ = value;
  } else {
    if (value.attr_type() == AttrType::NULLS) {
      // null: +0
      return RC::SUCCESS;
    }

    ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
          attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

    int result = value_.compare(value);
    if (result > 0) {
      value_ = value;
    }
  }
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value& value)
{
  // make count
  if (count_value_.attr_type() == AttrType::UNDEFINED) {
    count_value_ = new Value(static_cast<int>(0));
  }
  if (value.attr_type() != AttrType::NULLS) {
    count_value_.set_int(count_value_.get_int() + 1);
  }

  // make sum
  if (sum_value_.attr_type() == AttrType::UNDEFINED) {
    sum_value_ = value;
    return RC::SUCCESS;
  }
  
  if (sum_value_.attr_type() == AttrType::NULLS) {
    // 第一行合并的是 null
    sum_value_ = value;
  } else {
    if (value.attr_type() == AttrType::NULLS) {
      // null: +0
      return RC::SUCCESS;
    }

    ASSERT(value.attr_type() == sum_value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
          attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
    
    switch (value.attr_type())
    {
      case AttrType::INTS: {
        sum_value_.set_int(value.get_int() + sum_value_.get_int());
      } break;
      case AttrType::FLOATS: {
        sum_value_.set_float(value.get_float() + sum_value_.get_float());
      } break;
      default: {
        return RC::INTERNAL;
      }
    }
  }
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value& result)
{
  if (sum_value_.is_null()) {
    // 所有值都是null
    ASSERT(count_value_.get_int() == 0, "null but count() is not 0");
    result = sum_value_;
  } else {
    ASSERT(count_value_.get_int() != 0, "count() is 0 but divide 0");
    result = new Value(static_cast<float>(sum_value_.get_float() / count_value_.get_float()));
  }
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value& value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = new Value(static_cast<int>(0));
  }
  if (value.attr_type() != AttrType::NULLS) {
    value_.set_int(value_.get_int() + 1);
  }
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}