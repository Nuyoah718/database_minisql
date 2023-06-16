#include "executor/executors/index_scan_executor.h"
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}
    // : AbstractExecutor(exec_ctx), plan_(plan), filter_predicate_(plan->filter_predicate_) {}

void IndexScanExecutor::Init() {
//   /* get catalog */
//   CatalogManager *catalog = exec_ctx_->GetCatalog();

//   /* get table */
//   std::string t_name = plan_->GetTableName();
//   TableInfo *t_info = nullptr;
//   if (catalog->GetTable(t_name, t_info) != DB_SUCCESS) {
//     ASSERT(false, "Fail to get table.");
//   }

//   /* get schema */
//   Schema *schema = t_info->GetSchema();

//   /* for all indexeds_ find row_id */
//   int index_num = plan_->indexes_.size();
//   std::vector<std::vector<RowId>> r_id_indexes(index_num); // for each index
//   for (int i = 0; i < index_num; ++i) {
//     IndexInfo *i_info = plan_->indexes_[i];
//     Schema *index_schema = i_info->GetIndexKeySchema();
//     ASSERT(index_schema->GetColumnCount() == 1, "Only support one column index.");

//     /* check predicate on column with inex */
//     Index *idx = i_info->GetIndex();

//     Row key_tobe_filtered();
//     idx->ScanKey(key_tobe_filtered, r_id_indexes[i], exec_ctx_->GetTransaction());

//     auto column_is_valid = filter_predicate_->Evaluate(&key_tobe_filtered);
//     if (column_is_valid.CompareEquals(Field(kTypeInt, 1)) == CmpBool::kTrue) {
//       /* predicate is true */
//     }

//   }

//   /*  */
//   if (!plan_->need_filter_) {
//     /* set_intersection */
//   }




// // (2) scan indexes, 
// // (3) intersection 
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  // while (cur_ < size_rows_) {
  //   /* get next row to be filtered */
  //   RowId r_id = r_ids_[cur_++];
  //   Row row_tobe_filtered(r_id);
  //   if (!table_heap_->GetTuple(&row_tobe_filtered, nullptr)) { // GetTuple(row, txn)??
  //     ASSERT(false, "GetTuple Fails.");
  //   } 

  //   /* check predicate */
  //   auto is_valid = filter_predicate_->Evaluate(&row_tobe_filtered);
  //   if (is_valid.CompareEquals(Field(kTypeInt, 1)) == CmpBool::kTrue) {
  //     /* predicate is true */
  //     new(row) Row(row_tobe_filtered);
  //     new(rid) RowId(row_tobe_filtered.GetRowId());
  //     return true;
  //   }
  // }
  return false;
}
