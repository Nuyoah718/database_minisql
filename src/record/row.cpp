#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const
{
  uint32_t offset = 0;

  //写入 Row 的 Magic Number
  //Magic number 是一个预定义的常量
  uint32_t magic_number = ROW_MAGIC_NUM;
  memcpy(buf + offset, &magic_number, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  //写入字段数
  auto field_count = static_cast<uint32_t>(fields_.size());
  memcpy(buf + offset, &field_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  //写入空位图
  for (const auto &field : fields_)
  {
    bool is_null = field->IsNull();
    memcpy(buf + offset, &is_null, sizeof(bool));
    offset += sizeof(bool);
  }

  //写入每个字段
  for (const auto &field : fields_)
    offset += field->SerializeTo(buf + offset);

  return offset;
}


/**
 * TODO: Student Implement
 */
uint32_t Row::DeserializeFrom(char *buf, Schema *given_schema)
{
  //初始化偏移量和字段索引
  uint32_t offset = 0, field_index = 0;

  //从输入缓冲区读取字段数量
  uint32_t num_fields = MACH_READ_UINT32(buf);
  offset += sizeof(uint32_t);

  //读取空字段的数量
  uint32_t num_null_fields = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);

  //创建空字段指示器，初始化为0
  std::vector<uint32_t> null_field_indicator(num_fields, 0);

  //根据空字段数量设置空字段指示器
  for (field_index = 0; field_index < num_null_fields; field_index++)
  {
    null_field_indicator[MACH_READ_UINT32(buf + offset)] = 1;
    offset += sizeof(uint32_t);
  }

  //遍历所有字段，创建一个新的字段，并推入fields_向量
  for (field_index = 0; field_index < num_fields; field_index++)
  {
    //如果当前字段不为空，则进行反序列化
    fields_.push_back(ALLOC_P(heap_, Field)(given_schema->GetColumn(field_index)->GetType()));
    if (!null_field_indicator[field_index])
      offset += Field::DeserializeFrom(buf + offset, given_schema->GetColumn(field_index)->GetType(), &fields_[field_index], false);
  }

  //返回最后的偏移量
  return offset;
}


/**
 * TODO: Student Implement
 */
uint32_t Row::GetSerializedSize(Schema *schema) const
{
  uint32_t size = 0;

  //加上 Magic Number 和字段数的大小
  size += sizeof(uint32_t) * 2;

  //加上空位图的大小
  size += sizeof(bool) * fields_.size();

  //加上每个字段的大小
  for (const auto &field : fields_)
    size += field->GetSerializedSize();

  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) const
{
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns)
  {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
