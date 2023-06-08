#include "buffer/clock_replacer.h"

// 构造函数：初始化second_chance数组，pointer指针以及容量capacity
CLOCKReplacer::CLOCKReplacer(size_t num_pages)
    :second_chance(num_pages,State::EMPTY),
      pointer(0),
      capacity(num_pages) {}

// 析构函数：无操作
CLOCKReplacer::~CLOCKReplacer() {}

// Victim函数：寻找并返回一个victim frame id
bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  frame_id_t victim_frame_id = 0;

  // 遍历整个second_chance数组寻找victim
  for (size_t i = 0; i < 2 * capacity; ++i) {
    auto id = (pointer + i) % capacity;
    if (second_chance[id] == State::ACCESSED) {    // 如果状态为ACCESSED
      second_chance[id] = State::UNUSED;  // 给予第二次机会，设为UNUSED
    } else if (second_chance[id] == State::UNUSED && victim_frame_id == 0) { // 如果状态为UNUSED且没有找到victim
      victim_frame_id = id;  // 设置当前frame为victim
    }
  }

  if (victim_frame_id == 0) { // 如果所有的frame都已被访问，指定第一个UNUSED的frame为victim
    for (size_t i = 0; i < capacity; ++i) {
      auto id = (pointer + i) % capacity;
      if (second_chance[id] == State::UNUSED) {
        victim_frame_id = id;
        break;
      }
    }
  }

  // 如果找不到victim，返回false
  if (victim_frame_id == 0) {
    *frame_id = 0;
    return false;
  }

  // 将找到的victim frame的状态设置为EMPTY，更新pointer，并返回victim frame id
  second_chance[victim_frame_id] = State::EMPTY;
  pointer = (victim_frame_id + 1) % capacity;
  *frame_id = victim_frame_id;

  return true;
}

// Pin函数：将指定的frame从replacer中移除
void CLOCKReplacer::Pin(frame_id_t frame_id) {
  second_chance[frame_id % capacity] = State::EMPTY;
}

// Unpin函数：将指定的frame添加进replacer
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  second_chance[frame_id % capacity] = State::ACCESSED;
}

// Size函数：返回replacer当前的大小，即State不等于EMPTY的frame数量
size_t CLOCKReplacer::Size() {
  return count_if(second_chance.begin(), second_chance.end(), IsEmpty);
}

// IsEmpty函数：检查frame的State是否不等于EMPTY
bool CLOCKReplacer::IsEmpty(CLOCKReplacer::State& item) {
  return item != State::EMPTY;
}
