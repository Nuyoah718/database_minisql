#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
//向堆表中插入一条记录，插入记录后生成的RowId需要通过row对象返回（即row.rid_）
bool TableHeap::InsertTuple(Row &row, Transaction *txn)
{
  //首先需要找到一个可以存放新元组的页面
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr)
    return false;
  //插入该元组
  bool success = page->InsertTuple(row, txn, lock_manager_, log_manager_);
  //Unpin这一页面
  buffer_pool_manager_->UnpinPage(first_page_id_, true);
  return success;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn)
{
  //Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  //If the page could not be found, then abort the transaction.
  if (page == nullptr)
    return false;
  //Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
//将RowId为rid的记录old_row替换成新的记录new_row，并将new_row的RowId通过new_row.rid_返回
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn)
{
  //获取包含该元组的页面
  auto page = (TablePage *)buffer_pool_manager_->FetchPage(rid.GetPageId());

  //页面不存在，返回更新失败
  if (page == nullptr)
    return false;

  //新元组过大，无法存入页面，返回更新失败
  if (row.GetSize() > PAGE_SIZE)
  {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }

  //更新元组
  Row old_row(row);
  auto ret_state = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);

  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

  //根据更新结果返回
  switch (ret_state)
  {
    case TablePage::RetState::ILLEGAL_CALL: // 非法调用
    case TablePage::RetState::INSUFFICIENT_TABLE_PAGE: // 页面空间不足
    case TablePage::RetState::DOUBLE_DELETE: // 重复删除
      return false;
    case TablePage::RetState::SUCCESS: // 更新成功
      return true;
    default:
      return false;
  }
}


/**
 * TODO: Student Implement
 */
//从物理意义上删除这条记录
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn)
{
  //获取包含该元组的页面
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  //删除该元组
  page->ApplyDelete(rid, txn, log_manager_);
  //Unpin该页
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn)
{
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);

  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
//获取RowId为row->rid_的记录
bool TableHeap::GetTuple(Row *row, Transaction *txn)
{
  //获取包含该元组的页面
  auto page = (TablePage *)buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId());

  if (page == nullptr)
    return false;

  bool success = page->GetTuple(row, schema_, txn, lock_manager_);

  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);

  return success;
}


void TableHeap::DeleteTable(page_id_t page_id)
{
  if (page_id != INVALID_PAGE_ID)
  {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap

    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());

    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  }
  else
    DeleteTable(first_page_id_);
}

/**
 * TODO: Student Implement
 */
//获取堆表的首迭代器
TableIterator TableHeap::Begin(Transaction *txn)
{
  if (first_page_id_ == INVALID_PAGE_ID)
    //The table is empty, return an invalid iterator.
    return TableIterator(this, INVALID_ROWID, txn);

  TablePage *first_page = (TablePage *)buffer_pool_manager_->FetchPage(first_page_id_);
  if (!first_page)
    //The first page doesn't exist, return an invalid iterator.
    return TableIterator(this, INVALID_ROWID, txn);


  RowId first_row_id;
  if (!first_page->GetFirstTupleRid(&first_row_id))
  {
    //The first page is empty, return an invalid iterator.
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return TableIterator(this, INVALID_ROWID, txn);
  }

  //Unpin the page after we're done with it.
  buffer_pool_manager_->UnpinPage(first_page_id_, false);

  return TableIterator(this, first_row_id, txn);
}

/**
 * TODO: Student Implement
 */
//获取堆表的尾迭代器
TableIterator TableHeap::End()
{
  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr);
}
