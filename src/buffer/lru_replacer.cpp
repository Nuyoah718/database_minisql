#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages)
    :capacity(num_pages) {}

//默认析构函数，无需进行任何操作
LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
//替换（即删除）与所有被跟踪的页相比最近最少被访问的页（LRU 缓存替换策略）
//将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数frame_id中输出并返回true
//如果当前没有可以替换的元素则返回false
bool LRUReplacer::Victim(frame_id_t *outputFrameId) {
  //如果访问列表为空，返回false（不能替换）
  if (frameList.empty()) return false;

  //获取列表头部的帧id，即最少使用的帧（作为牺牲者）
  auto leastUsedFrame = frameList.front();
  frameList.pop_front();

  //在映射中查找这个最少使用的帧
  auto frameIterator = mapping.find(leastUsedFrame);
  if (frameIterator != mapping.end()) {
    //如果在映射中找到了，则删除它
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
//LRUReplacer 类的 Pin 方法，接收一个帧 ID，表示将该帧固定在内存中，即不会被替换出去
//如果帧 ID 在 LRUReplacer 中的映射表 mapping 中存在，则将其从 LRU 列表 visit_lst 中删除，并从 mapping 中删除该映射
//这样就可以保证该帧不会在后续的替换过程中被选中，从而被替换出去
//Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto least_itr = mapping.find(frame_id);    //首先根据帧的ID找到该帧的存储位置
  if (least_itr != mapping.end()) {
    //从 LRU 列表 visit_lst 中删除该帧
    frameList.erase(least_itr->second);

    //从 mapping 中删除该映射
    mapping.erase(least_itr);
  }
}

/**
 * TODO: Student Implement
 */
//将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉
//Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，使页帧对应的数据页能够在必要时被替换
void LRUReplacer::Unpin(frame_id_t frameId) {
  //反向操作Pin
  //如果映射的大小已经达到了容量上限，则不执行任何操作
  if (mapping.size() >= capacity)
    return;

  //查找目标帧id是否已经在映射中
  auto iterator = mapping.find(frameId);

  //如果目标帧id已经在映射中，也不执行任何操作
  if (iterator != mapping.end())
    return;

  //将目标帧id添加到访问列表的末尾
  frameList.push_back(frameId);

  //在映射中添加一个新的元素，其中键是目标帧id，值是指向访问列表末尾的迭代器
  mapping.insert({frameId, std::prev(frameList.end())});
}

/**
 * TODO: Student Implement
 */
//返回当前LRUReplacer中能够被替换的数据页的数量
size_t LRUReplacer::Size() {
  return mapping.size();    //直接返回mapping的size即可
}
