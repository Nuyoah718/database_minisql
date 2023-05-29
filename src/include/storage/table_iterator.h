#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator
{
 public:
  //构造函数和析构函数
  explicit TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn);
  TableIterator(const TableIterator &other);
  virtual ~TableIterator();

  //运算符重载
  inline bool operator==(const TableIterator &itr) const;    //检查两个迭代器是否相等
  inline bool operator!=(const TableIterator &itr) const;    //检查两个迭代器是否不相等
  const Row &operator*();                             //解引用运算符
  Row *operator->();                                  //成员访问运算符符
  TableIterator &operator++();                        //前自增运算符
  const TableIterator operator++(int);                //后自增运算符
  TableIterator &operator=(const TableIterator &itr) noexcept;

 private:
  //成员变量
  TableHeap *table_heap{};    //指向TableHeap对象的指针
  Row *current_row_point{};                 //指向表中当前行的指针
  Transaction *current_trans_point{};         //指向当前事务的指针

  //私有默认构造函数，以防止实例化没有必要的参数
  explicit TableIterator() = default;
};

#endif    //MINISQL_TABLE_ITERATOR_H
