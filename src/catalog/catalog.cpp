#include "catalog/catalog.h"

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
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
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
}

/**
* DONE: Student Implement
*/
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  /* get table_id */
  table_id_t this_t_id = next_table_id_;
  next_table_id_++;

  /* create new table */ 
  TableHeap *new_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);

  /* TABLE_META */
  /* set table_metadata */
  TableMetadata *t_meta = TableMetadata::Create(this_t_id, table_name, new_heap->GetFirstPageId(), schema);

  /* TABLE_INFO */
  /* set  table_info */
  TableInfo *t_info = TableInfo::Create();
  t_info->Init(t_meta, new_heap);

  /* CATALOG_MANAGER */
  /* set name */
  table_names_.emplace(table_name, this_t_id);
  /* add <table_id, table_info*> */
  tables_.emplace(this_t_id, t_info);

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
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
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
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}