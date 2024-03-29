#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

//Buffer Pool Manager 负责从Disk Manager中获取数据页并将它们存储在内存中
//并在必要时将脏页面转储到磁盘中（如需要为新的页面腾出空间）

//初始化静态常量字符：设置空页数量为0
static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

//默认构造函数：初始化Buffer Pool Manager
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  //初始化页面数组和LRU替换器
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);

  //初始化空闲列表
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

//默认析构函数：销毁Buffer Pool Manager
BufferPoolManager::~BufferPoolManager() {
  //遍历页面表，将所有页面刷新至磁盘
  for (auto &page : page_table_) {
    FlushPage(page.first);
  }

  // 释放页面数组和LRU替换器
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
//根据逻辑页号获取对应的数据页，如果该数据页不在内存中，则需要从磁盘中进行读取
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  //在页面表中查找请求的页面（P）
  auto itr = page_table_.find(page_id);


  frame_id_t frame_idx;
  Page *target_frame;

  //如果找到页面（P），对其进行引用，并立即返回
  if (itr != page_table_.end()) {
    frame_idx = itr->second;
    target_frame = &pages_[frame_idx];
    //页面被引用
    target_frame->pin_count_++;
    replacer_->Pin(frame_idx);
    return target_frame;
  }

  //如果没找到，先从空闲列表中寻找替换页面（R）
  if (!free_list_.empty()) {
    frame_idx = free_list_.front();
    free_list_.pop_front();
  }
  else {
    //如果空闲列表也没有，就从替换器中找
    if (!replacer_->Victim(&frame_idx))
      return nullptr;
  }

  //获取替换页面（R）
  target_frame = &pages_[frame_idx];

  //如果替换页面（R）为脏页，将其写回磁盘
  if (target_frame->IsDirty()) {
    disk_manager_->WritePage(target_frame->page_id_, target_frame->data_);
    target_frame->is_dirty_ = false;
  }

  //从页面表中删除脏页，插入新的页面（P）
  page_table_.erase(target_frame->page_id_);
  page_table_.insert({page_id, frame_idx});

  //更新页面（P）的元数据，从磁盘读取页面内容
  target_frame->page_id_ = page_id;
  //页面被引用
  target_frame->pin_count_++;
  replacer_->Pin(frame_idx);

  //从磁盘读取页面（P）的内容
  disk_manager_->ReadPage(target_frame->page_id_, target_frame->data_);

  //返回指向页面（P）的指针
  return target_frame;
}

/**
 * TODO: Student Implement
 */
//分配一个新的数据页，并将逻辑页号于page_id中返回
Page *BufferPoolManager::NewPage(page_id_t &page_id)
{
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  //检查缓冲池中是否所有页面都被引用
  size_t idx = 0;
  for (idx = 0; idx < pool_size_; idx++) {
    if (pages_[idx].GetPinCount() <= 0)
      break;
  }

  //如果所有页面都被引用，返回nullptr
  if (idx == pool_size_)
    return nullptr;

  frame_id_t frame_idx;
  Page *target_frame;

  //首先从空闲列表中获取
  if (!free_list_.empty()) {
    frame_idx = free_list_.front();
    free_list_.pop_front();
  }
  else {
    //如果空闲列表没有，从替换器中获取
    if (!replacer_->Victim(&frame_idx))
      return nullptr;
  }
  target_frame = &pages_[frame_idx];

  //如果页面为脏页，写回磁盘
  if (target_frame->IsDirty()) {
    disk_manager_->WritePage(target_frame->page_id_, target_frame->data_);
    target_frame->is_dirty_ = false;
  }

  //分配新的页面ID
  auto new_page_id = AllocatePage();

  //从页面表中删除旧的页面，插入新的页面
  page_table_.erase(target_frame->page_id_);
  page_table_.insert({new_page_id, frame_idx});

  //更新页面元数据，清零内存，增加页面的引用计数
  target_frame->page_id_ = new_page_id;
  target_frame->ResetMemory();
  target_frame->pin_count_++;
  replacer_->Pin(frame_idx);

  //设置页面ID并返回新的页面
  page_id = new_page_id;
  return target_frame;
}

/**
 * TODO: Student Implement
 */
//释放一个数据页
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  //检查页面是否在页面表中
  auto page_itr = page_table_.find(page_id);
  frame_id_t frame_idx;
  Page *target_page;
  
  //如果页面不存在，直接返回true
  if (page_itr == page_table_.end()) 
    return true;  
                                                   
  //如果页面存在
  frame_idx = page_itr->second;
  target_page = &pages_[frame_idx];

  //如果页面的引用计数不为0，返回false
  if (target_page->pin_count_ > 0) 
    return false;

  //如果页面为脏页，写回磁盘
  if (target_page->IsDirty()) {
    disk_manager_->WritePage(target_page->page_id_, target_page->data_);
    target_page->is_dirty_ = false;
  }

  //从页表中删除页面并释放页面
  page_table_.erase(target_page->page_id_);
  DeallocatePage(target_page->page_id_);

  //重置页面元数据并将页面ID添加到空闲列表
  target_page->ResetMemory();
  target_page->page_id_ = INVALID_PAGE_ID;
  target_page->pin_count_ = 0;
  target_page->is_dirty_ = false;
  
  //回收frame_idx
  free_list_.push_back(frame_idx);  

  return true;
}

/**
 * TODO: Student Implement
 */
//取消固定一个数据页
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  //在页面表中查找给定的页面ID
  auto itr = page_table_.find(page_id);
  frame_id_t frame_idx;
  Page *target_frame;

  //如果在页面表中未找到给定页面ID，直接返回true
  if (itr == page_table_.end())
    return true;

  //获取找到的帧ID和对应的页面帧
  frame_idx = itr->second;
  target_frame = &pages_[frame_idx];

  //根据参数设置页面帧的脏页状态
  target_frame->is_dirty_ = is_dirty;

  //如果页面帧的引用计数为0，返回false
  if (target_frame->pin_count_ == 0)
    return false;

  //减少页面帧的引用计数
  target_frame->pin_count_--;

  //如果页面帧的引用计数变为0，调用替换器的Unpin方法
  if (target_frame->pin_count_ == 0)
    replacer_->Unpin(frame_idx);

  return true;
}

/**
 * TODO: Student Implement
 */
//将数据页转储到磁盘中
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  //使用find()方法在page_table_中寻找对应的page_id
  auto table_itr = page_table_.find(page_id);

  //如果在page_table_中找不到page_id，则返回false
  if (table_itr == page_table_.end())
    return false;

  //提取出对应的frame_id和Page对象
  frame_id_t frame_id = table_itr->second;
  Page *target_page = &pages_[frame_id];

  //利用disk_manager_将对应的Page对象写入磁盘
  disk_manager_->WritePage(target_page->page_id_, target_page->data_);

  //页面被写入磁盘后，更新其状态为非脏页
  target_page->is_dirty_ = false;

  //如果一切正常，则返回true
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
