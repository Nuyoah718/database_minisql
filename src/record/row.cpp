#include "record/row.h"
#include <algorithm>
#include <cassert>

//序列化函数
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");    //确保schema不为空
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  //确保fields的大小和schema的列数匹配

  uint32_t offset = 0;

  //写入字段数量和null字段数量
  memcpy(buf, &fields_nums, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(buf + offset, &null_nums, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  //标记null字段
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull()) {
      memcpy(buf + offset, &i, sizeof(uint32_t));
      offset += sizeof(uint32_t);
    }
  }

  //序列化非null字段
  for (auto &field : fields_) {
    if (!field->IsNull()) offset += field->SerializeTo(buf + offset);
  }

  return offset;
}

//反序列化函数
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");    //确保schema不为空
  ASSERT(fields_.empty(), "Non empty field in row.");    //确保fields为空

  uint32_t offset = 0;

  //读取字段数量和null字段数量
  memcpy(&fields_nums, buf, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(&null_nums, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  //构建null bitmap
  std::vector<uint32_t> null_bitmap(fields_nums, 0);
  for (uint32_t i = 0; i < null_nums; i++) {
    uint32_t null_index;
    memcpy(&null_index, buf + offset, sizeof(uint32_t));
    null_bitmap[null_index] = 1;
    offset += sizeof(uint32_t);
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
  ASSERT(schema != nullptr, "Invalid schema before serialize.");    //确保schema不为空
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  //确保fields的大小和schema的列数匹配

  //如果fields为空，返回0
  if (fields_.empty()) return 0;

  uint32_t size = sizeof(uint32_t) * (2 + null_nums);
  //计算非null字段的序列化大小
  for (auto &field : fields_) {
    if (!field->IsNull())
      size += field->GetSerializedSize();
  }

  return size;
}
