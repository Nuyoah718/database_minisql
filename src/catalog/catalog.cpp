#include "catalog/catalog.h"
#include "page/index_roots_page.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * DONE: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  int fix_size = sizeof(uint32_t) * 3; // MAGIC_NUM, table_nums, index_nums
  int table_pages_cnt = table_meta_pages_.size();
  int index_pages_cnt = index_meta_pages_.size();
  int var_size = 8 * (table_pages_cnt + index_pages_cnt);

  return fix_size + var_size;
}

CatalogMeta::CatalogMeta() {}

/**
 * DONE: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    next_index_id_ = 0;
    next_table_id_ = 0;
    return;
  }

  /* Case 2:
   * 在后续重新打开数据库实例时，从数据库文件中加载所有的表和索引信息，
   * 构建TableInfo和IndexInfo信息置于内存中。
   */
  /** CATA_META **/
  Page *catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
  char *buf = catalog_meta_page->GetData();
  catalog_meta_->DeserializeFrom(buf);

  /** Construct EVERY table_info **/
  for (auto tableID_pageID:catalog_meta_->table_meta_pages_) {
    table_id_t t_id = tableID_pageID.first;
    page_id_t p_id = tableID_pageID.second;
    if (p_id == INVALID_PAGE_ID) {
      /* the end() */
      break;
    }
    LoadTable(t_id, p_id);
  }

  /** Construct EVERY index_info **/
  for (auto indexID_pageID:catalog_meta_->index_meta_pages_) {
    table_id_t i_id = indexID_pageID.first;
    page_id_t p_id = indexID_pageID.second;
    if (p_id == INVALID_PAGE_ID) {
      /* the end() */
      break;
    }
    LoadIndex(i_id, p_id);
  }
  
  buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, true);
}

CatalogManager::~CatalogManager() {
 /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
  **/
  FlushCatalogMetaPage();
  // delete catalog_meta_;
  // for (auto iter : tables_) {
  //   delete iter.second;
  // }
  // for (auto iter : indexes_) {
  //   delete iter.second;
  // }
}

/**
* DONE: Student Implement
*/
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  /* get table_id */
  ASSERT(next_table_id_ == catalog_meta_->GetNextTableId(), "should be same.");
  table_id_t this_t_id = next_table_id_;
  next_table_id_++;

  /* create new table */ 
  TableHeap *new_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);

  /** TABLE_META **/
  /* set table_metadata */
  TableMetadata *t_meta = TableMetadata::Create(this_t_id, table_name, new_heap->GetFirstPageId(), schema);

  /** TABLE_INFO **/
  /* set  table_info */
  TableInfo *t_info = TableInfo::Create();
  t_info->Init(t_meta, new_heap);

  /** CATALOG_MANAGER **/
  /* set name */
  table_names_.emplace(table_name, this_t_id);
  /* add <table_id, table_info*> */
  tables_.emplace(this_t_id, t_info);

  /** CATALOG_META **/
  /* serialize t_meta */
  page_id_t t_meta_p_id = INVALID_PAGE_ID;
  Page *t_meta_page = buffer_pool_manager_->NewPage(t_meta_p_id);
  char *t_meta_buf = t_meta_page->GetData();
  t_meta->SerializeTo(t_meta_buf);
  
  /* add <t_id, meta_p_id> to cata_meta */
  catalog_meta_->table_meta_pages_[this_t_id] = t_meta_p_id;
  catalog_meta_->table_meta_pages_[next_table_id_] = INVALID_PAGE_ID; // mark the end
  FlushCatalogMetaPage();

  /* Unpin t_meta page */
  buffer_pool_manager_->UnpinPage(t_meta_p_id, true);
  /* return values */
  table_info = t_info;
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto itr = table_names_.find(table_name);
  if (itr == table_names_.end()) {
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  }

  /* table is found */
  table_id_t t_id = itr->second;
  auto tables_itr = tables_.find(t_id);
  ASSERT(tables_itr != tables_.end(), "table_id in table_names should be in tables_.");

  table_info = tables_itr->second;
  
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  ASSERT(tables.size() == 0, "Input table should be empty.");
  for (auto itr = tables_.begin(); itr != tables_.end(); itr++) {
    tables.push_back(itr->second);
  }

  ASSERT(tables_.size() == tables.size(), "All TableInfo should be in vector.");
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  ASSERT(next_index_id_ == catalog_meta_->GetNextIndexId(), "should be same.");

  /* check table exists */
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  /* check index not exists */
  std::unordered_map<std::string, index_id_t>  index_pageId;
  auto itr = index_names_.find(table_name);
  if (itr != index_names_.end()) {
    /* table_name have index-pageid pairs */
    index_pageId = index_names_.at(table_name);
    if (index_pageId.find(index_name) != index_pageId.end()) {
      /* index_name is found */
      return DB_INDEX_ALREADY_EXIST;
    }
  }

  /* get TableInfo* to get schema */
  table_id_t t_id = table_names_[table_name];
  TableInfo *t_info = nullptr;
  if (GetTable(t_id, t_info) != DB_SUCCESS) {
    ASSERT(false, "GetTable Fails.");
  }
  Schema *t_schema = t_info->GetSchema();

  /** INDEX_META **/
  /* change vector<string> to vector<uint32> */
  std::vector<uint32_t> key_map(index_keys.size());
  for (int i = 0; i < index_keys.size(); ++i) {
    uint32_t idx = -1;
    if (t_schema->GetColumnIndex(index_keys[i], idx) != DB_SUCCESS) {
      /* when index_keys are not valid, return */
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map[i] = idx;
  }

  /** INDEX_META **/
  /* set this index_id */
  index_id_t this_i_id = next_index_id_;
  next_index_id_++;
  /* set index_metadata */
  IndexMetadata *i_meta = IndexMetadata::Create(this_i_id, index_name, t_id, key_map);

  /** INDEX_INFO **/
  /* set  index_info */
  IndexInfo *i_info = IndexInfo::Create();
  i_info->Init(i_meta, t_info, buffer_pool_manager_);

  /** CATALOG_MANAGER **/
  /* set index name */
  index_names_[table_name][index_name] = this_i_id;
  /* add <index_id, index_info*> */
  indexes_.emplace(this_i_id, i_info);

  /** CATALOG_META **/
  /* serialize i_meta */
  page_id_t i_meta_p_id = INVALID_PAGE_ID;
  Page *i_meta_page = buffer_pool_manager_->NewPage(i_meta_p_id);
  char *i_meta_buf = i_meta_page->GetData();
  i_meta->SerializeTo(i_meta_buf);
  
  /* add <i_id, meta_p_id> to cata_meta */
  catalog_meta_->index_meta_pages_[this_i_id] = i_meta_p_id;
  catalog_meta_->index_meta_pages_[next_index_id_] = INVALID_PAGE_ID; // mark the end
  FlushCatalogMetaPage();

  /* Unpin i_meta page */
  buffer_pool_manager_->UnpinPage(i_meta_p_id, true);
  /* return values */
  index_info = i_info;
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto outter_itr = index_names_.find(table_name);
  if (outter_itr == index_names_.end()) {
    index_info = nullptr;
    return DB_INDEX_NOT_FOUND;
  }
  auto t_index_names = outter_itr->second;
  auto inner_itr = t_index_names.find(index_name);
  if (inner_itr == t_index_names.end()) {
    index_info = nullptr;
    return DB_INDEX_NOT_FOUND;
  }

  /* index is found */
  index_id_t i_id = inner_itr->second;
  auto index_itr = indexes_.find(i_id);
  ASSERT(index_itr != indexes_.end(), "index_id in index_names should be in indexes_.");

  index_info = index_itr->second;
  
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  ASSERT(indexes.size() == 0, "Input indexes should be empty.");
  /* check table_name exists */
  auto itr = index_names_.find(table_name);
  if (itr == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  /* find add index_id of table_name */
  auto idxNames_pageId = itr->second;
  std::vector<index_id_t> idx_id_vec;
  for (auto pair : idxNames_pageId) {
    index_id_t idx_id = pair.second;
    idx_id_vec.push_back(idx_id);
  }

  /* construct vec<IndexInfo *> from vec<IndexID> */
  for (index_id_t i_id : idx_id_vec) {
    auto itr_i_info = indexes_.find(i_id);
    ASSERT(itr_i_info != indexes_.end(), "index id is found, so it must exists in indexes_ ");
    indexes.push_back(itr_i_info->second);
  }
  
  ASSERT(indexes.size() == indexes_.size(), "Index should be same size.");
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  /* use table_name to find table_id */
  if (table_names_.find(table_name) == table_names_.end()) {
    // table not found
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t t_id = table_names_.at(table_name);
  return DropTable(t_id);  
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  /* check index exists */
  IndexInfo *i_info_tobe_delete = nullptr;
  auto error_num = GetIndex(table_name, index_name, i_info_tobe_delete);
  if (error_num != DB_SUCCESS) {
    return error_num;
  }
  ASSERT(index_names_.find(table_name) != index_names_.end() 
        && index_names_[table_name].find(index_name) != index_names_[table_name].end(),
        "<Index_name, index_id> must exists.");

  /** CATALOG_MAGENER **/
  /* delete <t_name, i_name, i_id> in CatalogManage */
  index_id_t i_id_tobe_delete = index_names_[table_name][index_name];
  index_names_[table_name].erase(index_name);
  /* delete <i_id, i_info> in CatalogManager */
  indexes_.erase(i_id_tobe_delete);

  /** CATALOG_META **/
  /* delete <index_id, page_id> and delete this page_id in bpm */
  catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, i_id_tobe_delete);

  /** INDEX_ROOTS_PAGE **/
  auto *index_roots = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  index_roots->Delete(i_id_tobe_delete);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  
  /** IndexInfo* itself **/
  delete i_info_tobe_delete;
  
  return DB_SUCCESS;
}
  

/* only use in DropTable(const std::string &table_name) */
dberr_t CatalogManager::DropTable(table_id_t table_id) {
  /* check table exists */
  TableInfo *t_info_tobe_delete = nullptr;
  auto error_num = GetTable(table_id, t_info_tobe_delete);
  if (error_num != DB_SUCCESS) {
    return error_num;
  }

  /** CATALOG_MAGENER **/
  /* delete <t_name, t_id> in CatalogManage */
  //// table_names_.erase(table_name); // this line is in DropTable(table_name);
  /* delete <t_id, t_info> in CatalogManager */
  tables_.erase(table_id);

  /** CATALOG_META **/
  /* delete <table_id, page_id> and delete this page_id in bpm */
  catalog_meta_->DeleteTableMetaPage(buffer_pool_manager_, table_id);

  /** TableInfo* itself **/
  delete t_info_tobe_delete;

  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto *buf = reinterpret_cast<char *>(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
  catalog_meta_->SerializeTo(buf);

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  /* construct t_metadata */
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  char *buf = page->GetData(); // deserialize from
  TableMetadata *t_meta = nullptr;
  TableMetadata::DeserializeFrom(buf, t_meta);
  
  /* construct t_heap */
  TableHeap *t_heap = TableHeap::Create(buffer_pool_manager_, 
                      t_meta->GetFirstPageId(), t_meta->GetSchema(),
                      log_manager_, lock_manager_);
  
  /* construct table_info */
  TableInfo *t_info = TableInfo::Create();
  t_info->Init(t_meta, t_heap);
  
  /** CATALOG_MANAGER **/
  // add <t_name, t_id> and <t_id, t_info>
  table_names_.emplace(t_info->GetTableName(), table_id);
  tables_.emplace(table_id, t_info); 

  buffer_pool_manager_->UnpinPage(page_id, false);
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  /* short name */
  index_id_t i_id = index_id;
  page_id_t p_id = page_id;

  /* construct i_metadata */
  Page *page = buffer_pool_manager_->FetchPage(p_id);
  char *buf = page->GetData(); // deserialize from
  IndexMetadata *i_meta = nullptr;
  IndexMetadata::DeserializeFrom(buf, i_meta);

  /* get root_page_id from INVALID_PAGE_ID */
  auto *idx_roots = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t index_root_page_id = INVALID_PAGE_ID;
  
  if (!idx_roots->GetRootId(i_id, &index_root_page_id)) {
    ASSERT(false, "fail to find index root page.");
  }

  /* Get table_info of t_id */
  table_id_t t_id = i_meta->GetTableId();
  TableInfo *t_info = tables_[t_id];

  /* reconstruct Index* in IndexInfo::Init() */
  IndexInfo *i_info = IndexInfo::Create();
  i_info->Init(i_meta, t_info, buffer_pool_manager_);

  /** CATALOG_MANAGER **/
  /* insert <i_id, i_info> */
  indexes_.emplace(i_id, i_info);
  /* insert <t_name, <i_name, i_id>> */
  index_names_[t_info->GetTableName()][i_info->GetIndexName()] = i_id;

  buffer_pool_manager_->UnpinPage(p_id, false);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * DONE: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto itr = tables_.find(table_id);
  if (itr == tables_.end()) {
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  }

  /* table is found */
  table_info = itr->second;
  
  return DB_SUCCESS;
}