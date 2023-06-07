#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
//分配一个空闲页，并通过page_offset返回所分配的空闲页位于该段中的下标（从0开始）
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  //检查所请求的页面是否已经被分配，或者是否超出了最大允许的页面数量
  if (allocate_pages.test(page_offset) || page_offset >= 8 * MAX_CHARS)
    return false;    //如果已经超出了最大允许的页面数量，则无法分配该页面
  else {
    allocate_pages.set(page_offset, 1);
    return true;    //将相应的位图位（page_offset）设置为1，表示该页已被分配
  }
}

/**
 * TODO: Student Implement
 */
//回收已经被分配的页
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  //检查请求回收的页面是否已经被分配
  if (allocate_pages.test(page_offset)) {
    allocate_pages.set(page_offset, 0);    //将相应的位图位设置为0，表示该页已被回收
    next_free_page_ = page_offset;    //更新下一个可用的页面索引
    page_allocated_--;    //已分配的页面数量减1
    return true;    //返回true表示成功回收页面
  }
  else
    return false;    //返回false表示无法回收该页面（可能是因为它未被分配）
}

/**
 * TODO: Student Implement
 */
//判断给定的页是否是空闲（未分配）的
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  //如果相应的位图位为0，表示该页是空闲的，返回true；否则返回false
  return !allocate_pages.test(page_offset);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

//实例化不同页面大小的BitmapPage类
template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
