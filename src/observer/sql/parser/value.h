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
// Created by Wangyunlai 2023/6/27
//

#pragma once

#include <string>

/**
 * @brief 属性的类型
 *
 */
enum class AttrType
{
  UNDEFINED,
  CHARS,     ///< 字符串类型
  INTS,      ///< 整数类型(4字节)
  FLOATS,    ///< 浮点数类型(4字节)
  DATES,
  NULLS,
  BOOLEANS,  ///< boolean类型，当前不是由parser解析出来的，是程序内部使用的
};

const char *attr_type_to_string(AttrType type);
AttrType    attr_type_from_string(const char *s);

/**
 * @brief 属性的值
 *
 */
class Value final
{
public:
  Value() = default;

  Value(AttrType attr_type, char *data, int length = 4) : attr_type_(attr_type) { this->set_data(data, length); }

  explicit Value(int val);
  explicit Value(float val);
  explicit Value(bool val);
  explicit Value(const char *s, int len = 0);

  Value(const Value &other)            = default;
  Value &operator=(const Value &other) = default;
  Value &operator=(const Value *other)
  {
    if (this != other) {
      attr_type_ = other->attr_type_;
      length_ = other->length_;
      str_value_ = other->str_value_;

      // 根据类型拷贝union的值
      switch (attr_type_) {
        case AttrType::INTS:
          num_value_.int_value_ = other->num_value_.int_value_;
          break;
        case AttrType::FLOATS:
          num_value_.float_value_ = other->num_value_.float_value_;
          break;
        case AttrType::BOOLEANS:
          num_value_.bool_value_ = other->num_value_.bool_value_;
          break;
        case AttrType::DATES:
          num_value_.int_value_ = other->num_value_.int_value_;
        default:
          // 处理其他类型或UNDEFINED类型
          break;
      }
    }
    return *this;
  }

  void set_type(AttrType type) { this->attr_type_ = type; }
  void set_data(char *data, int length);
  void set_data(const char *data, int length) { this->set_data(const_cast<char *>(data), length); }
  void set_int(int val);
  void set_float(float val);
  void set_boolean(bool val);
  void set_string(const char *s, int len = 0);
  void set_date(int val);
  void set_null();
  void set_value(const Value &value);

  bool is_null() const;

  std::string to_string() const;

  int compare(const Value &other) const;

  const char *data() const;
  int         length() const { return length_; }

  AttrType attr_type() const { return attr_type_; }

public:
  /**
   * 获取对应的值
   * 如果当前的类型与期望获取的类型不符，就会执行转换操作
   */
  int         get_int() const;
  float       get_float() const;
  std::string get_string() const;
  bool        get_boolean() const;

private:
  AttrType attr_type_ = AttrType::UNDEFINED;
  int      length_    = 0;

  union
  {
    int   int_value_;
    float float_value_;
    bool  bool_value_;
  } num_value_;
  std::string str_value_;
};