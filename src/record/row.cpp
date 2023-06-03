#include "record/row.h"
#include <algorithm>

uint32_t Row::SerializeTo(char *buf, Schema *schema) const
{
  uint32_t offset = 0;

  auto field_count = static_cast<uint32_t>(fields_.size());
  memcpy(buf + offset, &field_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  uint32_t null_fields_count = std::count_if(fields_.begin(), fields_.end(), [](const auto &field) { return field->IsNull(); });
  memcpy(buf + offset, &null_fields_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull()) {
      memcpy(buf + offset, &i, sizeof(uint32_t));
      offset += sizeof(uint32_t);
    }
  }

  for (const auto &field : fields_) {
    if (!field->IsNull()) offset += field->SerializeTo(buf + offset);
  }

  return offset;
}


uint32_t Row::DeserializeFrom(char *buf, Schema *given_schema)
{
  uint32_t offset = 0, field_index = 0;

  uint32_t num_fields = MACH_READ_UINT32(buf);
  offset += sizeof(uint32_t);

  uint32_t num_null_fields = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);

  std::vector<uint32_t> null_field_indicator(num_fields, 0);

  for (field_index = 0; field_index < num_null_fields; field_index++)
  {
    null_field_indicator[MACH_READ_UINT32(buf + offset)] = 1;
    offset += sizeof(uint32_t);
  }

  for (field_index = 0; field_index < num_fields; field_index++)
  {
    fields_.push_back(ALLOC_P(heap_, Field)(given_schema->GetColumn(field_index)->GetType()));
    if (!null_field_indicator[field_index])
      offset += Field::DeserializeFrom(buf + offset, given_schema->GetColumn(field_index)->GetType(), &fields_[field_index], false);
  }

  return offset;
}


uint32_t Row::GetSerializedSize(Schema *schema) const
{
  uint32_t size = sizeof(uint32_t) * 2;
  if(fields_.empty())
    return size;

  uint32_t null_fields_count = std::count_if(fields_.begin(), fields_.end(), [](const auto &field) { return field->IsNull(); });
  size += sizeof(uint32_t) * null_fields_count;

  for (const auto &field : fields_) {
    if (!field->IsNull())
      size += field->GetSerializedSize();
  }

  return size;
}
