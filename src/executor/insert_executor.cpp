#include "executor/executors/insert_executor.h"
#include "values.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next(Row *row, RowId *rid) {
  // 检查是否有子执行器，并尝试获取下一行记录和对应的行标识
  if (child_executor_ && child_executor_->Next(row, rid)) {
    // 获取表名
    const std::string &table_name = plan_->GetTableName();

    // 从目录管理器获取表信息
    CatalogManager *catalog_manager = exec_ctx_->GetCatalog();
    TableInfo *table_info;
    auto result = catalog_manager->GetTable(table_name, table_info);
    if (result != DB_SUCCESS) {
      return false;
    }

    // 从行记录中提取字段值
    const std::vector<Field> &fields = reinterpret_cast<const std::vector<Field> &>(row->GetFields());

    // 将行记录插入表中
    if (!table_info->GetTableHeap()->InsertTuple(*row, reinterpret_cast<Transaction *>(rid))) {
      return false;
    }

    // 插入所有索引
    std::vector<IndexInfo *> index_infos;
    catalog_manager->GetTableIndexes(table_name, index_infos);
    for (uint32_t i = 0; i < index_infos.size(); i++) {
      const Schema *index_key_schema = index_infos[i]->GetIndexKeySchema();
      const std::vector<Column *> &index_cols = index_key_schema->GetColumns();
      std::vector<Field> index_key_fields;
      for (uint32_t j = 0; j < index_key_schema->GetColumnCount(); j++) {
        uint32_t col_pos;
        table_info->GetSchema()->GetColumnIndex(index_cols[j]->GetName(), col_pos);
        index_key_fields.push_back(fields[col_pos]);
      }
      Row index_key_row(index_key_fields);
      result = index_infos[i]->GetIndex()->InsertEntry(index_key_row, *rid, nullptr);
      if (result != DB_SUCCESS) {
        return false;
      }
    }

    return true;
  }

  return false;
}
