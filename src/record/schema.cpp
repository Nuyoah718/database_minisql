#include "record/schema.h"

/**
 * TODO: Student Implement
 */
//将Schema对象序列化到buf中，返回序列化后的字节数
uint32_t Schema::SerializeTo(char *buf) const
{
  uint32_t ofs = 0;
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);    //将SCHEMA_MAGIC_NUM写入buf中
  ofs += sizeof(uint32_t);    //偏移量加上uint32_t类型的字节数
  MACH_WRITE_UINT32(buf + ofs, columns_.size());    //将columns_的长度写入buf中
  ofs += sizeof(uint32_t);    //偏移量再加上uint32_t类型的字节数

  //遍历columns_，将每个Column对象进行序列化，并写入buf中
  for (auto &itr : columns_)
    ofs += itr->SerializeTo(buf + ofs);
  return ofs;    //返回序列化后的字节数
}


/**
 * TODO: Student Implement
 */
//获取Schema对象序列化后的字节数
uint32_t Schema::GetSerializedSize() const
{
  uint32_t size = 0;

  //遍历columns_，获取每个Column对象序列化后的字节数，并累加到size中
  for (auto &itr : columns_)
    size += itr->GetSerializedSize();

  //返回计算得到的序列化后的总字节数（要加上SCHEMA_MAGIC_NUM和columns_长度两个uint32_t类型的字节数）
  return size + 2 * sizeof(uint32_t);
}


/**
 * TODO: Student Implement
 */
//从buf中反序列化出Schema对象，并将其赋值给传入的指针schema，并返回反序列化后的字节数
uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema)
{
  auto num = MACH_READ_FROM(uint32_t, buf);    //从buf中读出SCHEMA_MAGIC_NUM
  ASSERT(num == Schema::SCHEMA_MAGIC_NUM, "Schema magic num error.");    //检查读出的SCHEMA_MAGIC_NUM是否正确
  uint32_t ofs = sizeof(uint32_t);    //偏移量加上uint32_t类型的字节数
  auto col_size = MACH_READ_UINT32(buf + ofs);    //从buf中读出columns_的长度
  ofs += sizeof(uint32_t);    //偏移量再加上uint32_t类型的字节数
  std::vector<Column *> columns;    //定义一个Column指针类型的向量

  //遍历buf中的数据，反序列化出每个Column对象，并将其添加到columns向量中
  for (auto i = 0u; i < col_size; i++)
  {
    Column *col;
    ofs += Column::DeserializeFrom(buf + ofs, col);    //反序列化出一个Column对象
    columns.push_back(col);    //将反序列化出的Column对象添加到columns向量中
  }

  //生成一个新的 Schema 对象
  schema = new Schema(columns);

  //返回从缓冲区中读取的字节数
  return ofs;
}
