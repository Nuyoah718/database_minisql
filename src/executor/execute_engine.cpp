#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "parser/minisql_lex.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    //Row row{};    //**模块五请注意，此处进行了修改，移除此行代码，改为在while循环中再创建Row对象
    //但是这样存在性能问题（因为每次循环都会创建新的Row对象），但是对性能的影响有多大，仍在研究中
    //在row.h中不再提供默认的无参构造函数，以防止没有有效的数据
    //但是需要保证在每次循环的时候都有新的fields数据用于创建新的Row对象，如果不是这样，请对其再次进行修改
    while (true) {
      std::vector<Field> fields; // Create a vector to hold fields
      // Use your method to populate fields vector. This is just a placeholder
      // PopulateFields(fields);
      Row row(fields); // Create a new row using the fields
      if (!executor->Next(&row, &rid)) break;
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 * 建立数据库
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
    cout << "ok";
  }
  dbs_[db_name] = new DBStorageEngine(db_name);
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 *
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  auto it = dbs_.find(db_name);
  if (it == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  string db_file_name_ = "./databases/" + db_name;
  LOG(INFO) << "remove db file: " << db_file_name_;
  remove(db_file_name_.c_str());

  dbs_.erase(it);

  // If the current db is the one we are dropping, set the current db to empty.
  if (current_db_ == db_name) {
    current_db_ = "";
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  // Store the records into a vector so that we can change the output more easily.
  vector<string> db_names;
  for (auto it : dbs_) {
    db_names.push_back(it.first);
  }
  // Print the db names in the most naive way.
  LOG(INFO) << "print all dbs";
  for (auto it : db_names) {
    cout << it << "\n";
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if(dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  LOG(INFO) << "use database " << db_name;
  current_db_ = db_name;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  // If no database is selected, we should reject the request.
  if (current_db_.empty()) {
    LOG(WARNING) << "No database selected.";
    return DB_FAILED;
  }
  vector<TableInfo *> table_info_vec;
  context->GetCatalog()->GetTables(table_info_vec);
  // Store the records into a vector so that we can change the output more easily.
  vector<string> table_names;
  for (auto it : table_info_vec) {
    table_names.push_back(it->GetTableName());
  }

  // Print the table names in the most naive way.
  for (auto it : table_names) {
    cout << it << "\n";
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  // If no database is selected, we should reject the request.
  if (current_db_.empty()) {
    LOG(WARNING) << "No database selected.";
    return DB_FAILED;
  }

  string table_name(ast->child_->val_);
  // Iterate the column list and parse the columns.
  auto col_list_node = ast->child_->next_;
  vector<string> primary_key_list;
  unordered_set<string> primary_key_set;
  vector<string> col_names;
  vector<TypeId> col_types;
  vector<bool> col_is_unique;
  vector<int> col_manage_len; // Only used when the column type is char.
  vector<int> col_col_id; // Only used when the column type is char.
  int cnt = 0;
  for (auto it = col_list_node->child_; it != nullptr; it = it->next_) {
    if (it->type_ == kNodeColumnDefinition) {
      // If column is unique.
      col_is_unique.emplace_back(it->val_ != nullptr);
      // Column definition.
      col_names.emplace_back(it->child_->val_);
      col_col_id.emplace_back(cnt++);
      // Parse the column type.
      string col_type_name(it->child_->next_->val_);
      if (col_type_name == "int") {
        col_types.emplace_back(kTypeInt);
        col_manage_len.emplace_back(0);
      } else if (col_type_name == "char") {
        int len = atoi(it->child_->next_->child_->val_);
        if (len < 0) {
          LOG(WARNING) << "Meet invalid char length: " << len;
          return DB_FAILED;
        }
        col_types.emplace_back(kTypeChar);
        col_manage_len.emplace_back(len);
      } else if (col_type_name == "float") {
        col_types.emplace_back(kTypeFloat);
        col_manage_len.emplace_back(0);
      } else {
        LOG(WARNING) << "Meet invalid column type: " << col_type_name;
        // col_types.emplace_back(kTypeInvalid);
        // col_manage_len.emplace_back(0);
        return DB_FAILED;
      }
    } else if (it->type_ == kNodeColumnList) {
      // Primary key definition.
      for (auto pk_it = it->child_; pk_it != nullptr; pk_it = pk_it->next_) {
        primary_key_list.push_back(string(pk_it->val_));
        primary_key_set.insert(string(pk_it->val_));
      }
    }
  }

  // Generate the columns vector.
  vector<Column *> columns;
  bool should_manage = false;
  for (int i = 0; i < col_names.size(); ++i) {
    // LOG(INFO) << "Load column " << col_names[i] << "\n";
    if (primary_key_set.find(col_names[i]) != primary_key_set.end()) {
      // That means this column is a primary key, which is unique and not nullable.
      if (col_types[i] == kTypeChar) {
        columns.push_back(new Column(col_names[i], col_types[i], col_manage_len[i], i, false, true));
        should_manage = true;
      } else {
        columns.push_back(new Column(col_names[i], col_types[i], i, false, true));
      }
    } else {
      if (col_types[i] == kTypeChar) {
        columns.push_back(new Column(col_names[i], col_types[i], col_manage_len[i], i, false, col_is_unique[i]));
        should_manage = true;
      } else {
        columns.push_back(new Column(col_names[i], col_types[i], i, false, col_is_unique[i]));
      }

    }
    // If the type is char, we should set the length.
  }

  // Create table schema.
  Schema * schema = new Schema(columns, should_manage);
  // This is the output of the create table command which is useless here.
  TableInfo * table_info;
  // Create the table.
  dberr_t err = context->GetCatalog()->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  if (err != DB_SUCCESS) {
    return err;
  }

  // Create index on primary key.
  // LOG(INFO) << "Try to generate index on primary key.";
  // This is the output of the create index command which is useless here.
  if (primary_key_list.size() != 0) {
    IndexInfo * index_info;
    err = context->GetCatalog()->CreateIndex(
        table_info->GetTableName(),
        table_name + "_PK_IDX",
        primary_key_list,
        context->GetTransaction(),
        index_info,
        "bptree"
    );
    if (err != DB_SUCCESS) {
      return err;
    }
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  // If no database is selected, we should reject the request.
  if (current_db_.empty()) {
    LOG(WARNING) << "No database selected.";
    return DB_FAILED;
  }

  // Drop the table.
  string table_name(ast->child_->val_);
  dberr_t err = context->GetCatalog()->DropTable(table_name);
  if (err != DB_SUCCESS) {
    return err;
  }

  // Drop the index.
  std::vector<IndexInfo *> index_info_vec;
  context->GetCatalog()->GetTableIndexes(table_name, index_info_vec);
  for (auto index_info : index_info_vec) {
    err = context->GetCatalog()->DropIndex(table_name, index_info->GetIndexName());
    if (err != DB_SUCCESS) {
      return err;
    }
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()){
    LOG(WARNING) << "No database selected.";
    return DB_FAILED;
  }

  //get all tables to get indexes.
  std::vector<TableInfo *> table_info_vec;
  context->GetCatalog()->GetTables(table_info_vec);

  //Use map to store the table name and its index info.
  map< string, vector<IndexInfo *> > table_index_vec_pair;
  for(auto table_info : table_info_vec){
    string table_name = table_info->GetTableName();
    std::vector<IndexInfo *> index_info_vec;
    context->GetCatalog()->GetTableIndexes(table_name, index_info_vec);
    table_index_vec_pair[table_name] = index_info_vec;
  }

  for(auto it : table_index_vec_pair){
    cout << "@ table \"" << it.first << "\",we have indexes:" << endl;
    for(auto index_info : it.second){
      cout << "   "<< index_info->GetIndexKeySchema() << "on columns: ";
      for(auto col : index_info->GetIndexKeySchema()->GetColumns()){
        cout << "【" << col->GetName() << "]";
      }
      cout << endl;
    }
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  Transaction *txn = context->GetTransaction();
  //get the index name, table name, index type, and the related columns.
  string index_name (ast->child_->val_);
  string table_name(ast->child_->next_->val_);
  string index_type("bptree");
  vector<string> column_names;
  for(auto it = ast->child_->next_->next_->child_; it != nullptr; it = it->next_){
    column_names.push_back(it->val_);
  }
  if (ast->child_->next_->next_->next_ != nullptr) {
    index_type = string(ast->child_->next_->next_->next_->child_->val_);
  }

  //get tableinfo
  TableInfo * table_info;
  dberr_t err = context->GetCatalog()->GetTable(table_name, table_info);
  if (err != DB_SUCCESS){
    return err;
  }

  IndexInfo * index_info;
  err = context->GetCatalog()->CreateIndex(
      table_name,
      index_name,
      column_names,
      txn,
      index_info,
      index_type
  );
  if (err != DB_SUCCESS){
    return err;
  }

  Index *idx = index_info->GetIndex();
  IndexSchema *i_schema = index_info->GetIndexKeySchema();
  TableHeap *table_heap = table_info->GetTableHeap();
  // build key_map of index
  std::vector<uint32_t> key_map;
  for (int i = 0; i < i_schema->GetColumnCount(); ++i) {
    key_map.push_back(i_schema->GetColumn(i)->GetTableInd());
  }
  // insert index entry
  for (auto row_itr = table_heap->Begin(txn); row_itr != table_heap->End(); ++row_itr) {
    Row row = *row_itr;

    Row key();
  }


  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  // If no database is selected, we should reject the request.
  if (current_db_.empty()) {
    LOG(WARNING) << "No database selected.";
    return DB_FAILED;
  }

  // Get the index name.
  string index_name(ast->child_->val_);

  // Find the related table name.
  string table_name;
  vector<TableInfo *> table_info_vec;
  dberr_t err = context->GetCatalog()->GetTables(table_info_vec);
  if (err != DB_SUCCESS) {
    return err;
  }
  // Search for the table.
  for (auto table_info : table_info_vec) {
    IndexInfo * index_info;
    err = context->GetCatalog()->GetIndex(table_info->GetTableName(), index_name, index_info);
    if (err == DB_SUCCESS) {
      table_name = table_info->GetTableName();
      break;
    }
  }

  if (table_name.empty()) {
    LOG(WARNING) << "No related table found.";
    return DB_INDEX_NOT_FOUND;
  }

  // Drop index.
  context->GetCatalog()->DropIndex(table_name, index_name);

  return DB_FAILED;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif

  std::string filename(ast->child_->val_);
  std::ifstream exefstream(filename);
  if (!exefstream.is_open()) {
    LOG(ERROR) << "Failed to open '" << filename << "'";
    return DB_FAILED;
  }

  std::vector<char> cmd(1024);
  size_t tmp_counter = 0;

  while (true) {
    char tmp_char = exefstream.get();
    if (exefstream.eof()) {
      return DB_SUCCESS;
    }

    cmd[tmp_counter++] = tmp_char;

    if (tmp_counter >= cmd.size()) {
      LOG(ERROR) << "Buffer overflow";
      return DB_FAILED;
    }

    if (tmp_char != ';') continue;

    cmd[tmp_counter] = 0;
    YY_BUFFER_STATE bp = yy_scan_string(cmd.data());
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state.";
      exit(1);
    }

    yy_switch_to_buffer(bp);

    MinisqlParserInit();

    yyparse();

    if (MinisqlParserGetError()) {
      printf("%s\n", MinisqlParserGetErrorMessage());
    }

    auto res = this->Execute(MinisqlGetParserRootNode());
    if (res != DB_SUCCESS) {
      LOG(ERROR) << "Execution of command failed.";
      return res;
    }

    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    if (context->flag_quit_) {
      printf("bye!\n");
      break;
    }
  }

  return DB_FAILED;
}


/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
