#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
 */
//将Column对象序列化到给定的缓冲区中，返回写入缓冲区的字节数。
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;

  //写入 Column 的 Magic Number
  WriteUint32(buf + offset, COLUMN_MAGIC_NUM);
  offset += sizeof(uint32_t);

  //写入 Column 名称字符串长度
  uint32_t name_length = name_.size() * sizeof(char); // 字符串长度（字节）
  WriteUint32(buf + offset, name_length);
  offset += sizeof(uint32_t);

  //写入 Column 名称字符串
  WriteString(buf + offset, name_);
  offset += name_length;

  //写入 Column 的类型、长度、表索引、是否可空、是否唯一性等元数据信息
  WriteTypeId(buf + offset, type_);
  offset += sizeof(TypeId);
  WriteUint32(buf + offset, len_);
  offset += sizeof(uint32_t);
  WriteUint32(buf + offset, table_ind_);
  offset += sizeof(uint32_t);
  WriteBool(buf + offset, nullable_);
  offset += sizeof(bool);
  WriteBool(buf + offset, unique_);
  offset += sizeof(bool);

  //返回写入缓冲区的总字节数
  return offset;
}

/**
 * TODO: Student Implement
 */
//获取Column对象序列化后的大小
uint32_t Column::GetSerializedSize() const {
  uint32_t name_length = name_.size() * sizeof(char);
  return sizeof(uint32_t) + sizeof(uint32_t) + name_length + sizeof(TypeId) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(bool) + sizeof(bool);
}

/**
 * TODO: Student Implement
 */
//从缓冲区中反序列化出一个Column对象
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset = 0;

  //检查 Magic Number 是否正确
  uint32_t magic_num = ReadUint32(buf + offset);
  offset += sizeof(uint32_t);
  if (magic_num != COLUMN_MAGIC_NUM) {
    LOG(ERROR) << "Invalid magic number for deserialize Column object: " << magic_num;
    return 0;
  }

  //读取 Column 名称字符串长度
  uint32_t name_length = ReadUint32(buf + offset);
  offset += sizeof(uint32_t);

  //读取 Column 名称字符串
  std::string name = ReadString(buf + offset);
  offset += name_length;

  //读取 Column 的类型、长度、表索引、是否可空、是否唯一性等元数据信息
  TypeId type = ReadTypeId(buf + offset);
  offset += sizeof(TypeId);
  uint32_t len = ReadUint32(buf + offset);
  offset += sizeof(uint32_t);
  uint32_t table_ind = ReadUint32(buf + offset);
  offset += sizeof(uint32_t);
  bool nullable = ReadBool(buf + offset);
  offset += sizeof(bool);
  bool unique = ReadBool(buf + offset);
  offset += sizeof(bool);

  //生成一个新的 Column 对象
  if (type == TypeId::kTypeChar)
    column = new Column(name, type, len, table_ind, nullable, unique);
  else
    column = new Column(name, type, table_ind, nullable, unique);

  //设置 Column 对象的长度信息
  column->len_ = len;

  //返回从缓冲区中读取的字节数
  return offset;
}

//写入32位无符号整数
void Column::WriteUint32(char *buf, uint32_t value) {
  memcpy(buf, &value, sizeof(uint32_t));
}

//写入布尔值
void Column::WriteBool(char *buf, bool value) {
  memcpy(buf, &value, sizeof(bool));
}

//写入列的类型信息
void Column::WriteTypeId(char *buf, TypeId value) {
  memcpy(buf, &value, sizeof(TypeId));
}

//写入字符串
void Column::WriteString(char *buf, const std::string &value) {
  const char *str = value.c_str();
  uint32_t len = value.size() * sizeof(char);
  WriteUint32(buf, len);
  strncpy(buf + sizeof(uint32_t), str, len);
  buf[len] = '\0';
}

//从缓冲区读取32位无符号整数
uint32_t Column::ReadUint32(const char *buf) {
  uint32_t value;
  memcpy(&value, buf, sizeof(uint32_t));
  return value;
}

//从缓冲区读取布尔值
bool Column::ReadBool(const char*buf) {
  bool value;
  memcpy(&value, buf, sizeof(bool));
  return value;
}

//从缓冲区读取列的类型信息
TypeId Column::ReadTypeId(const char *buf) {
  TypeId value;
  memcpy(&value, buf, sizeof(TypeId));
  return value;
}

//从缓冲区读取字符串
std::string Column::ReadString(const char *buf) {
  uint32_t len = ReadUint32(buf);
  const char *str = buf + sizeof(uint32_t);
  return std::string(str, len / sizeof(char));
}
