#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *table_heap, RowId rid,Transaction*txn)
    :table_heap(table_heap),
      row(new Row(rid)),
      txn(txn) {
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    this->table_heap->GetTuple(row, txn);
  }
}

TableIterator::TableIterator(const TableIterator &other)
    :table_heap(other.table_heap),
     row(new Row(*other.row)),
      txn(other.txn) {

}

TableIterator::~TableIterator() { delete row; 
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row->GetRowId().Get() == itr.row->GetRowId().Get();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  //  ASSERT(false, "Not implemented yet.");
  ASSERT(*this != table_heap->End(), "TableHeap iterator out of range, invalid dereference.");
  return *row;
}

Row *TableIterator::operator->() {
  ASSERT(*this != table_heap->End(), "TableHeap iterator out of range, invalid dereference.");
  return row;
}

TableIterator &TableIterator::operator++() {
  BufferPoolManager *buffer_pool_manager = table_heap->buffer_pool_manager_;
  auto cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(row->GetRowId().GetPageId()));
  cur_page->RLatch();
  assert(cur_page != nullptr);  // all pages are pinned

  RowId next_tuple_rid;
  if (!cur_page->GetNextTupleRid(row->GetRowId(),
                                 &next_tuple_rid)) {  // end of this page
    while (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
      auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(cur_page->GetNextPageId()));
      cur_page->RUnlatch();
      buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
      cur_page = next_page;
      cur_page->RLatch();
      if (cur_page->GetFirstTupleRid(&next_tuple_rid)) {
        break;
      }
    }
  }
  row = new Row(next_tuple_rid);

  if (*this != table_heap->End()) {
    table_heap->GetTuple(row ,nullptr);
  }
  // release until copy the tuple
  cur_page->RUnlatch();
  buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
  return *this;
}

TableIterator TableIterator::operator++(int) {  // postfix
  TableIterator clone(*this);
  ++(*this);
  return clone;
}
