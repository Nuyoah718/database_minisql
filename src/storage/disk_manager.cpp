#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    //如果文件不存在，就创建一个新的文件
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    //重新打开文件
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open())
      throw std::exception();
  }
  //加载元数据（meta_data_)
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close()
{
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
//从磁盘中分配一个空闲页，并返回空闲页的逻辑页号
page_id_t DiskManager::AllocatePage() {
  //获取元数据页，里面包含了文件的元数据信息
  auto* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  //获取文件中的 Extent 数量
  uint32_t extent_count = meta_page->GetExtentNums();

  //遍历每一个 Extent，找到第一个还有空闲页的 Extent
  uint32_t extent_index;
  for (extent_index = 0; extent_index < extent_count; extent_index++) {
    if (meta_page->extent_used_page_[extent_index] < DiskManager::BITMAP_SIZE)
      break;
  }

  //如果每一个 Extent 都没有空闲页了，那么就需要新分配一个 Extent
  if (extent_index == extent_count) {
    //如果已经分配的 Extent 数量已经达到最大限制，那么就无法再分配了
    if (extent_index == (PAGE_SIZE - 8) / 4)
      return INVALID_PAGE_ID;

    //分配新的 Extent
    meta_page->num_allocated_pages_++;
    meta_page->num_extents_++;
    meta_page->extent_used_page_[extent_index] = 1;

    //将新分配的 Extent 的位图清空，表示所有页都是空闲的
    char buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
    uint32_t ofs = 0;
    bitmap->AllocatePage(ofs);
    WritePhysicalPage(1 + extent_index * (DiskManager::BITMAP_SIZE + 1), buf);

    //返回新分配的页的编号
    return meta_page->num_allocated_pages_ - 1;
  }
  //如果存在一个有空闲页的 Extent，那么就在该 Extent 中分配一个页
  char buf[PAGE_SIZE];
  ReadPhysicalPage(1 + extent_index * (DiskManager::BITMAP_SIZE + 1), buf);
  auto* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buf);
  uint32_t page_index;

  for (page_index = 0; page_index < BitmapPage<4096>::GetMaxSupportedSize(); page_index++) {
    if (bitmap->IsPageFree(page_index))
      break;
  }
  
  //更新元数据信息，表示已经分配了一个新的页
  meta_page->num_allocated_pages_++;
  meta_page->extent_used_page_[extent_index]++;

  //将新分配的页的状态设置为已分配
  bitmap->AllocatePage(page_index);
  WritePhysicalPage(1 + extent_index * (DiskManager::BITMAP_SIZE + 1), buf);

  //返回新分配的页的编号
  return meta_page->num_allocated_pages_ - 1;
}

/**
 * TODO: Student Implement
 */
//释放磁盘中逻辑页号对应的物理页
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if (IsPageFree(logical_page_id))
    return;    //如果页面已经释放，直接返回
  else {
    //获取文件元数据页面
    auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

    //读取位图页面
    char bitmap_buf[PAGE_SIZE];
    const uint32_t bitmap_page_id = 1 + logical_page_id / DiskManager::BITMAP_SIZE * (DiskManager::BITMAP_SIZE + 1);
    ReadPhysicalPage(bitmap_page_id, bitmap_buf);
    auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buf);

    //释放页面
    const uint32_t page_id = logical_page_id % DiskManager::BITMAP_SIZE;
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[logical_page_id / DiskManager::BITMAP_SIZE]--;
    bitmap->DeAllocatePage(page_id);
    WritePhysicalPage(bitmap_page_id, bitmap_buf);
  }
}

/**
 * TODO: Student Implement
 */
//判断该逻辑页号对应的数据页是否空闲
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  const int kPageSize = PAGE_SIZE;
  const int kBitmapSize = DiskManager::BITMAP_SIZE;
  const int kBitmapEntrySize = kBitmapSize + 1;

  //计算逻辑页号所在的位图页面
  const int bitmap_page_num = 1 + (logical_page_id / kBitmapSize) * kBitmapEntrySize;

  //读取位图页面，并检查逻辑页号是否空闲
  char bitmap_page[kPageSize];
  ReadPhysicalPage(bitmap_page_num, bitmap_page);
  auto *bitmap = reinterpret_cast<BitmapPage<kPageSize> *>(bitmap_page);
  const int bitmap_index = logical_page_id % kBitmapSize;
  const bool is_free = bitmap->IsPageFree(bitmap_index);

  return is_free;
}

/**
 * TODO: Student Implement
 */
//用于将逻辑页号转换成物理页号
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  const int kBitmapSize = DiskManager::BITMAP_SIZE;
  const int kBitmapEntrySize = kBitmapSize + 1;

  //计算物理页号
  const page_id_t bitmap_page_num = 1 + (logical_page_id / kBitmapSize) * kBitmapEntrySize;
  const page_id_t physical_page_num = logical_page_id + bitmap_page_num + 1;

  return physical_page_num;
}

//获取文件的大小
int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

//读取物理页面
void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  //检查是否读取的范围超出了当前文件的长度
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    //根据偏移设置读取指针
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

//写入物理页面
void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
