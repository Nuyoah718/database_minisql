#include "executor/executors/seq_scan_executor.h"

SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  /* get table_heap_.Begin() */
  string t_name = plan_->GetTableName();
  auto *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info;
  if (catalog->GetTable(t_name, table_info) != DB_SUCCESS) {
    ASSERT(false, sprintf("table_name: %s does not exists.", t_name.c_str()));
  }
  table_heap_ = table_info->GetTableHeap();
  cur_ = table_heap_->Begin(exec_ctx_->GetTransaction());

  /* get predicate */
  filter_predicate_ = plan_->GetPredicate();
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  while (cur_ != table_heap_->End()) {
    Row row_tobe_filtered = *cur_;
    auto is_valid = filter_predicate_->Evaluate(&row_tobe_filtered);
    cur_++;
    if (is_valid.CompareEquals(Field(kTypeInt, 1)) == CmpBool::kTrue) {
      /* predicate is true */
      new(row) Row(row_tobe_filtered);
      new(rid) RowId(row_tobe_filtered.GetRowId());
      return true;
    }
  }
  
  return false;
}
