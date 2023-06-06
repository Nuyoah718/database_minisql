#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages)
    : capacity(num_pages) {}

CLOCKReplacer::~CLOCKReplacer() {}

//获取牺牲页
bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if (clock_list.empty())
    return false;

  for (auto it = clock_list.begin(); it != clock_list.end(); ++it) {
    if (!clock_status[*it]) {
      *frame_id = *it;
      clock_list.erase(it);
      clock_status.erase(*frame_id);
      return true;
    }
    else {
      //重置当前状态为false（表示未被使用）
      clock_status[*it] = false;
    }
  }

  //所有的帧都被访问，则选取列表中的第一个帧作为牺牲者
  *frame_id = clock_list.front();
  clock_list.pop_front();
  clock_status.erase(*frame_id);

  return true;
}

//将帧置为锁定状态
void CLOCKReplacer::Pin(frame_id_t frame_id) {
  auto it = clock_status.find(frame_id);
  if (it != clock_status.end()) {
    clock_list.remove(frame_id);
    clock_status.erase(it);
  }
}

//取消对帧的锁定
void CLOCKReplacer::Unpin(frame_id_t frame_id)
{
  auto it = clock_status.find(frame_id);
  if (it == clock_status.end() && clock_list.size() < capacity) {
    clock_list.push_back(frame_id);
    clock_status[frame_id] = false; // Set as UNUSED
  }
}

//计算那些状态不等于EMPTY的数量
size_t CLOCKReplacer::Size()
{
  return clock_status.size();
}
