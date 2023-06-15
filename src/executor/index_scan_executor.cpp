#include "executor/executors/index_scan_executor.h"
/**
* DOING: Tao Chengjian Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  return false;
}
