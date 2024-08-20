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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(Table *table, Value *values, int value_amount, FieldMeta *fields, FilterStmt *filter_stmt)
    : table_(table), values_(values), value_amount_(value_amount), fields_(fields), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, UpdateSqlNode &update, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  const char *table_name = update.relation_name.c_str();
  if (nullptr == table_name) {
    LOG_WARN("invalid argument. relation name is null");
    return RC::INVALID_ARGUMENT;
  }
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  
  // check fields type
  // update t1 set c1 = 1;
  // 1.检查 表t1 有没有c1 列
  // 2.检查 c1 列的类型 与 1 是否匹配
  std::string update_field_name = update.attribute_name;
  const FieldMeta *field_meta = table->table_meta().field(update_field_name.c_str());
  if (nullptr == field_meta) {
    LOG_INFO("no such field in table: %s.%s", table_name, update_field_name.c_str());
    return RC::SCHEMA_FIELD_MISSING;
  }

  if (field_meta->type() == update.value.attr_type()) {
    if (field_meta->type() == AttrType::CHARS && field_meta->len() <= update.value.length()) {
      // 检查字符串长度
      return RC::INVALID_ARGUMENT;
    }
  } else if (update.value.attr_type() == AttrType::NULLS && field_meta->nullable()) {
    // ok
  } else {
    LOG_WARN("update field type mismatch. table=%s",table_name);
    return RC::INVALID_ARGUMENT;
  }

  unordered_map<string, Table *> table_map;
  table_map.insert({table_name, table});
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(
    db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // everything alright
  stmt = new UpdateStmt(table, &(update.value), 1, const_cast<FieldMeta*>(field_meta), filter_stmt);
  return RC::SUCCESS;
}
