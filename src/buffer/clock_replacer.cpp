#include "buffer/clock_replacer.h"

/**
 * 实现了模块一的Bonus目标：
 * 除LRU Replacer外，实现一种新的缓冲区替换算法（如Clock Replacer）
 * @author Weilin Chang
 * @date 06.07.2023
 */

//构造函数，初始化页面数量
CLOCKReplacer::CLOCKReplacer(size_t num_pages)
    : capacity(num_pages) {}

//析构函数
CLOCKReplacer::~CLOCKReplacer() {}

//获取牺牲页
bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  //如果列表为空，则返回false
  if (clock_list.empty())
    return false;

  //遍历列表寻找未被使用的帧
  auto it = clock_list.begin();
  while (it != clock_list.end()) {
    if (!clock_status[*it]) {
      *frame_id = *it;
      it = clock_list.erase(it);
      clock_status.erase(*frame_id);
      return true;
    }
    else {
      //如果该帧被使用，则重置状态为false（表示未被使用）
      clock_status[*it] = false;
      ++it;
    }
  }

  //如果所有帧都被使用，则选择列表中的第一个帧作为牺牲者
  *frame_id = clock_list.front();
  clock_list.pop_front();
  clock_status.erase(*frame_id);

  return true;
}

//将帧置为锁定状态
void CLOCKReplacer::Pin(frame_id_t frame_id) {
  //如果该帧存在，则移除该帧并清除其状态
  auto it = clock_status.find(frame_id);
  if (it != clock_status.end()) {
    clock_list.remove(frame_id);
    clock_status.erase(it);
  }
}

//取消对帧的锁定
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  //如果该帧不存在并且列表未满，则将该帧添加到列表，并设置状态为未使用
  auto it = clock_status.find(frame_id);
  if (it == clock_status.end() && clock_list.size() < capacity) {
    clock_list.push_back(frame_id);
    clock_status[frame_id] = false;    //设置为未使用
  }
}

//计算那些状态不等于EMPTY的数量
size_t CLOCKReplacer::Size() {
  //返回非空状态的帧数量
  return clock_status.size();
}
