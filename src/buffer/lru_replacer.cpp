#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages)
    :capacity(num_pages) {}

//默认析构函数，无需进行任何操作
LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *outputFrameId)
{
  //如果访问列表为空，返回false
  if (frameList.empty()) return false;

  //获取列表头部的帧id，即最少使用的帧
  auto leastUsedFrame = frameList.front();
  frameList.pop_front();

  //在映射中查找这个最少使用的帧
  auto frameIterator = mapping.find(leastUsedFrame);
  if (frameIterator != mapping.end())
  {
    //如果在映射中找到了，删除它
    mapping.erase(frameIterator);
    //设置输出的帧id为最少使用的帧
    *outputFrameId = leastUsedFrame;
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
/* LRUReplacer 类的 Pin 方法，接收一个帧 ID，表示将该帧固定在内存中，即不会被替换出去；
    如果帧 ID 在 LRUReplacer 中的映射表 mapping 中存在，则将其从 LRU 列表 visit_lst 中删除，并从 mapping 中删除该映射；
    这样就可以保证该帧不会在后续的替换过程中被选中，从而被替换出去 */
void LRUReplacer::Pin(frame_id_t frame_id)
{
  auto least_itr = mapping.find(frame_id);
  if (least_itr != mapping.end())
  {
    //从 LRU 列表 visit_lst 中删除该帧
    frameList.erase(least_itr->second);

    //从 mapping 中删除该映射
    mapping.erase(least_itr);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frameId)
{
  //反向操作Pin
  //如果映射的大小已经达到了容量上限，则不执行任何操作
  if (mapping.size() >= capacity) return;

  //查找目标帧id是否已经在映射中
  auto iterator = mapping.find(frameId);

  //如果目标帧id已经在映射中，也不执行任何操作
  if (iterator != mapping.end()) return;

  //将目标帧id添加到访问列表的末尾
  frameList.push_back(frameId);

  //在映射中添加一个新的元素，其中键是目标帧id，值是指向访问列表末尾的迭代器
  mapping.insert({frameId, std::prev(frameList.end())});
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size()
{
  return mapping.size();
}
