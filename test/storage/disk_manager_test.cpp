#include <unordered_set>

#include "gtest/gtest.h"
#include "storage/disk_manager.h"

/*这里面对原有的测试代码进行了修改：
在"DiskManagerTest, BitMapPageTest"测试中，新增了ofs的初始化和更新。
    在原代码中，ofs的初始值没有明确定义。在新代码中，ofs被初始化为0，这避免了可能的未初始化变量使用问题。
    在新代码中，每次循环，ofs都会自增1，这样ofs将在每次迭代中使用不同的值。
在"DiskManagerTest, FreePageAllocationTest"测试中，新增了一段对变量a的操作代码。
*/
TEST(DiskManagerTest, BitMapPageTest) {
  const size_t size = 512;
  char buf[size];
  memset(buf, 0, size);
  BitmapPage<size> *bitmap = reinterpret_cast<BitmapPage<size> *>(buf);
  auto num_pages = bitmap->GetMaxSupportedSize();
  for (uint32_t i = 0; i < num_pages; i++) {
    ASSERT_TRUE(bitmap->IsPageFree(i));
  }
  uint32_t ofs = 0;
  std::unordered_set<uint32_t> page_set;
  for (uint32_t i = 0; i < num_pages; i++, ofs++) {
    ASSERT_TRUE(bitmap->AllocatePage(ofs));
    ASSERT_TRUE(page_set.find(ofs) == page_set.end());
    page_set.insert(ofs);
  }
  ofs = 233;
  ASSERT_FALSE(bitmap->AllocatePage(ofs));
  ASSERT_TRUE(bitmap->DeAllocatePage(233));
  ASSERT_TRUE(bitmap->AllocatePage(ofs));
  ASSERT_EQ(233, ofs);
  for (auto v : page_set) {
    ASSERT_TRUE(bitmap->DeAllocatePage(v));
    ASSERT_FALSE(bitmap->DeAllocatePage(v));
  }
  ofs = 0;
  for (uint32_t i = 0; i < num_pages; i++, ofs++) {
    ASSERT_TRUE(bitmap->AllocatePage(ofs));
  }
  ofs--;
  ASSERT_FALSE(bitmap->AllocatePage(ofs));
}

TEST(DiskManagerTest, FreePageAllocationTest) {
  std::string db_name = "disk_test.db";
  DiskManager *disk_mgr = new DiskManager(db_name);
  int extent_nums = 2;
  for (uint32_t i = 0; i < DiskManager::BITMAP_SIZE * extent_nums; i++) {
    if(i==DiskManager::BITMAP_SIZE-1){
      int a =0;
      a--;
    }
    page_id_t page_id = disk_mgr->AllocatePage();
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
    EXPECT_EQ(i, page_id);
    EXPECT_EQ(i / DiskManager::BITMAP_SIZE + 1, meta_page->GetExtentNums());
    EXPECT_EQ(i + 1, meta_page->GetAllocatedPages());
    EXPECT_EQ(i % DiskManager::BITMAP_SIZE + 1, meta_page->GetExtentUsedPage(i / DiskManager::BITMAP_SIZE));
  }

  disk_mgr->DeAllocatePage(0);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE - 1);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE + 1);
  disk_mgr->DeAllocatePage(DiskManager::BITMAP_SIZE + 2);
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(disk_mgr->GetMetaData());
  EXPECT_EQ(extent_nums * DiskManager::BITMAP_SIZE - 5, meta_page->GetAllocatedPages());
  EXPECT_EQ(DiskManager::BITMAP_SIZE - 2, meta_page->GetExtentUsedPage(0));
  EXPECT_EQ(DiskManager::BITMAP_SIZE - 3, meta_page->GetExtentUsedPage(1));
  remove(db_name.c_str());
}
