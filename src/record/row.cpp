#include "record/row.h"
#include <algorithm>
#include <cassert>

//序列化函数
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  //确保schema不为空
  ASSERT(schema != nullptr, "Invalid schema before serialize.");

  //确保fields的大小和schema的列数匹配
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  uint32_t offset = 0;

  //写入字段数量和null字段数量
  offset = WriteToBuffer(buf, offset, fields_nums);
  offset = WriteToBuffer(buf, offset, null_nums);

  //标记null字段
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull()) {
      offset = WriteToBuffer(buf, offset, i);
    }
  }

  //序列化非null字段
  for (auto &field : fields_) {
    if (!field->IsNull()) {
      offset += field->SerializeTo(buf + offset);
    }
  }

  return offset;
}

//反序列化函数
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  //确保schema不为空
  ASSERT(schema != nullptr, "Invalid schema before serialize.");

  //确保fields为空
  ASSERT(fields_.empty(), "Non empty field in row.");

  uint32_t offset = 0;

  //读取字段数量和null字段数量
  offset = ReadFromBuffer(buf, offset, fields_nums);
  offset = ReadFromBuffer(buf, offset, null_nums);

  //构建null bitmap
  std::vector<uint32_t> null_bitmap(fields_nums, 0);
  for (uint32_t i = 0; i < null_nums; i++) {
    uint32_t null_index;
    offset = ReadFromBuffer(buf, offset, null_index);
    null_bitmap[null_index] = 1;
  }

  //反序列化非null字段
  for (uint32_t i = 0; i < fields_nums; i++) {
    auto field = ALLOC_P(heap_, Field)(schema->GetColumn(i)->GetType());
    fields_.push_back(field);
    if (!null_bitmap[i]) {
      offset += field->DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &fields_[i], false);
    }
  }

  return offset;
}

//获取序列化大小
uint32_t Row::GetSerializedSize(Schema *schema) const {
  //确保schema不为空
  ASSERT(schema != nullptr, "Invalid schema before serialize.");

  //确保fields的大小和schema的列数匹配
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  //如果fields为空，返回0
  if (fields_.empty())
    return 0;

  uint32_t size = sizeof(uint32_t) * (2 + null_nums);

  //计算非null字段的序列化大小
  for (auto &field : fields_) {
    if (!field->IsNull())
      size += field->GetSerializedSize();
  }

  return size;
}

void Row::SetFields(const std::vector<Field>& fields) {
  fields_.clear();
  for (auto &field : fields) {
    void *buf = heap_->Allocate(sizeof(Field));
    fields_.push_back(new (buf) Field(field));
    if (field.IsNull())
      null_nums++;
  }

  fields_nums = fields.size();
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row.SetFields(fields);
}

//辅助函数，用于向缓冲区中的简化读写操作
uint32_t Row::WriteToBuffer(char *buf, uint32_t offset, const uint32_t &value) const {
  memcpy(buf + offset, &value, sizeof(uint32_t));
  return offset + sizeof(uint32_t);
}

uint32_t Row::ReadFromBuffer(char *buf, uint32_t offset, uint32_t &value) const {
  memcpy(&value, buf + offset, sizeof(uint32_t));
  return offset + sizeof(uint32_t);
}
