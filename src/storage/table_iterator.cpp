#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn)
    : table_heap(table_heap), row(table_heap->GetRow(rid, txn)), txn(txn) {}

TableIterator::TableIterator(const TableIterator &other)
    : table_heap(other.table_heap), row(other.row), txn(other.txn) {}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
  return this->table_heap == itr.table_heap && this->row == itr.row && this->txn == itr.txn;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  ASSERT(this->row != nullptr, "Row pointer is null.");
  return *this->row;
}

Row *TableIterator::operator->() {
  ASSERT(this->row != nullptr, "Row pointer is null.");
  return this->row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    this->table_heap = itr.table_heap;
    this->row = itr.row;
    this->txn = itr.txn;
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  ASSERT(this->row != nullptr && this->txn != nullptr, "Row or transaction pointer is null.");
  this->row = this->table_heap->GetNextRow(this->row, this->txn);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator old_itr = *this;
  ++*this;
  return old_itr;
}
