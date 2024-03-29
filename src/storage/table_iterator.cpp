#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

//使用初始化列表来初始化成员
//如果传入的rid有效，则通过调用table_heap的GetTuple获取元组
//如果传入的rid无效，则不进行操作
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Transaction* txn)
    : table_heap(table_heap), row(new Row(rid)), txn(txn) {
  if (rid.GetPageId() != INVALID_PAGE_ID)
    this->table_heap->GetTuple(row, txn);
}

//拷贝构造函数，创建一个新的TableIterator，并复制other的所有状态
TableIterator::TableIterator(const TableIterator &other)
    : table_heap(other.table_heap), row(other.row ? new Row(*other.row) : nullptr), txn(other.txn) {}

//析构函数，释放动态分配的Row
TableIterator::~TableIterator() {
 // if (row != nullptr) {
    delete row;
   // row = nullptr;
  //}
}

TableIterator &TableIterator::operator=(const TableIterator &other) noexcept {
  if (this != &other) {
    table_heap = other.table_heap;
    txn = other.txn;
    if (row != nullptr) {
      delete row;
    }
    row = other.row ? new Row(*other.row) : nullptr;
  }
  return *this;
}

//使用GetRowId的返回值，判断两个TableIterator是否相等
bool TableIterator::operator==(const TableIterator &itr) const {
  return this->row->GetRowId().Get() == itr.row->GetRowId().Get();
}

//使用operator==，判断两个TableIterator是否不等
bool TableIterator::operator!=(const TableIterator &itr) const {
  return !this->operator==(itr);
}

//重载*操作符，返回当前Row的引用
//在解引用前，先检查当前迭代器是否处于有效状态
const Row &TableIterator::operator*() {
  ASSERT(this->operator!=(table_heap->End()), "TableHeap iterator out of range, invalid dereference.");
  return *this->row;
}

//重载->操作符，返回当前Row的指针
//在返回指针前，先检查当前迭代器是否处于有效状态
Row *TableIterator::operator->() {
  ASSERT(this->operator!=(table_heap->End()), "TableHeap iterator out of range, invalid dereference.");
  return this->row;
}

//重载++操作符
TableIterator& TableIterator::operator++() {
  BufferPoolManager* buffer_pool_manager = table_heap->buffer_pool_manager_;
  auto cur_page = reinterpret_cast<TablePage*>(buffer_pool_manager->FetchPage(row->GetRowId().GetPageId()));
  cur_page->RLatch();
  assert(cur_page != nullptr);    //所有页面均已固定

  RowId next_tuple_rid;
  if (!cur_page->GetNextTupleRid(row->GetRowId(), &next_tuple_rid)) {
    while (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
      //获取下一页的页面ID，并使用buffer_pool_manager获取对应的页面。
      auto next_page = reinterpret_cast<TablePage*>(buffer_pool_manager->FetchPage(cur_page->GetNextPageId()));
      cur_page->RUnlatch();
      //解除当前页面的固定状态，将cur_page更新为下一页，对新的页面再进行读锁定
      buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
      cur_page = next_page;
      cur_page->RLatch();
      if (cur_page->GetFirstTupleRid(&next_tuple_rid)) {
        break;    //如果新的当前页面存在第一个元组，则跳出循环
      }
    }
  }
  delete row;    //释放之前的行，避免内存泄露
  row = new Row(next_tuple_rid);    //使用下一个元组的行ID创建新的行对象

  if (*this != table_heap->End()) {    //如果迭代器不等于table_heap的末尾迭代器
    table_heap->GetTuple(row, nullptr);    //获取新行对应的元组数据
  }
  cur_page->RUnlatch();
  buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);

  return *this;
}

//重载++(int)操作符
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this);    //先保存当前迭代器状态，再执行自增，最后返回保存的旧状态
  ++(*this);
  return temp;
}
