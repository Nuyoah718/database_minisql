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
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
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
page_id_t DiskManager::AllocatePage() 
{
    // Get file metadata page
    auto metaPage = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
    auto numExtents = metaPage->GetExtentNums();

    // Find an extent with free pages
    uint32_t extentIndex;
    for (extentIndex = 0; extentIndex < numExtents; extentIndex++) {
        if (metaPage->extent_used_page_[extentIndex] < DiskManager::BITMAP_SIZE) {
            break;
        }
    }

    // If all extents are full, allocate a new extent
    if (extentIndex == numExtents) {
        if (numExtents == (PAGE_SIZE - 8) / 4) {
            // Cannot allocate more extents
            return INVALID_PAGE_ID;
        }

        // Allocate a new extent
        metaPage->num_allocated_pages_++;
        metaPage->num_extents_++;
        metaPage->extent_used_page_[extentIndex] = 1;

        // Initialize new bitmap page
        char buf[PAGE_SIZE];
        memset(buf, 0, PAGE_SIZE);
        auto bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buf);
        bitmap->AllocatePage(0);

        // Write new bitmap page to disk
        WriteBitmapPage(1 + extentIndex * (DiskManager::BITMAP_SIZE + 1), bitmap);

        return metaPage->num_allocated_pages_ - 1;
    }

    // Find a free page within the extent
    auto bitmapPageIndex = extentIndex * (DiskManager::BITMAP_SIZE + 1) + 1;
    auto bitmapPage = GetBitmapPage(bitmapPageIndex);

    uint32_t pageIndex;
    for (pageIndex = 0; pageIndex < DiskManager::BITMAP_SIZE; pageIndex++) {
        if (bitmapPage->IsPageFree(pageIndex)) {
            break;
        }
    }

    // Allocate the page
    metaPage->num_allocated_pages_++;
    metaPage->extent_used_page_[extentIndex]++;
    bitmapPage->AllocatePage(pageIndex);

    // Write updated bitmap page to disk
    WriteBitmapPage(bitmapPageIndex, bitmapPage);

    return metaPage->num_allocated_pages_ - 1;
}

// Get bitmap page
BitmapPage<PAGE_SIZE>* DiskManager::GetBitmapPage(uint32_t bitmapPageIndex) {
    char pageBuffer[PAGE_SIZE];
    ReadBitmapPage(bitmapPageIndex, pageBuffer);
    return reinterpret_cast<BitmapPage<PAGE_SIZE>*>(pageBuffer);
}
// Read bitmap page
void DiskManager::ReadBitmapPage(uint32_t pageIndex, char* pageBuffer) {
    ReadPhysicalPage(pageIndex, pageBuffer);
}

// Write bitmap page
void DiskManager::WriteBitmapPage(uint32_t pageIndex, BitmapPage<PAGE_SIZE>* bitmapPage) {
    WritePhysicalPage(pageIndex, reinterpret_cast<char*>(bitmapPage));
}
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) 
{
  if (IsPageFree(logical_page_id)) 
    return;
  else 
  {
    // Get file metadata page
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

    // Read bitmap page
    char buf[PAGE_SIZE];
    ReadPhysicalPage(1 + logical_page_id / DiskManager::BITMAP_SIZE * (DiskManager::BITMAP_SIZE + 1), buf);
    BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);

    // Deallocate page
    uint32_t page_id = logical_page_id % DiskManager::BITMAP_SIZE;
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[logical_page_id / DiskManager::BITMAP_SIZE]--;
    bitmap->DeAllocatePage(page_id);
    WritePhysicalPage(1 + logical_page_id / DiskManager::BITMAP_SIZE * (DiskManager::BITMAP_SIZE + 1), buf);
  }

  // Check if page is free
  bool IsPageFree(uint32_t logical_page_id) {
    return IsPageFree(logical_page_id);
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char buf[PAGE_SIZE];
  uint32_t temp = logical_page_id / DiskManager::BITMAP_SIZE;
  ReadPhysicalPage(1 + temp * (DiskManager::BITMAP_SIZE + 1), buf);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
  bool result = bitmap->IsPageFree(logical_page_id % DiskManager::BITMAP_SIZE);
  return result;
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) 
{
  return logical_page_id + logical_page_id / DiskManager::BITMAP_SIZE + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
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
