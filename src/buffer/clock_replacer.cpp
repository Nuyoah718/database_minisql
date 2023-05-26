#include "buffer/clock_replacer.h"

//构造函数
ClockReplacer::ClockReplacer(size_t num_pages)
    :replacer(num_pages,State::EMPTY),
      index(0),
      max_size(num_pages) {}

//析构函数
ClockReplacer::~ClockReplacer() 
{
  //无需执行任何操作
}

//获取牺牲页
bool ClockReplacer::Victim(frame_id_t *frame_id) 
{
  size_t not_empty_count = 0, counter;
  frame_id_t chosen_frame_id = 0;

  //遍历找出第一个可用的牺牲页
  for (counter = 0; counter < max_size; counter++) 
  {
    auto id = (index + counter) % max_size;
    if (replacer[id] == State::EMPTY)
      continue;
    else if (replacer[id] == State::ACCESSED) 
    {
      not_empty_count++;
      replacer[id] = State::UNUSED;  //重置为未使用状态
    } 
    else if (replacer[id] == State::UNUSED) 
    {
      not_empty_count++;
      //获取第一个牺牲页
      chosen_frame_id = (chosen_frame_id != 0) ? chosen_frame_id : id;
    }
  }

  //如果全部为空，返回false
  if (not_empty_count == 0) 
  {
    frame_id = nullptr;
    return false;
  }

  if (chosen_frame_id == 0) 
  {
    for (counter = 0; counter < max_size; counter++) 
    {
      auto id = (index + counter) % max_size;
      if (replacer[id] == State::UNUSED) 
      {
        chosen_frame_id = id;
        break;
      }
    }
  }

  //设置所选页为空
  replacer[chosen_frame_id] = State::EMPTY;
  index = chosen_frame_id;
  *frame_id = chosen_frame_id;

  return true;
}

//将帧置为锁定状态
void ClockReplacer::Pin(frame_id_t frame_id) 
{
  //从替换器中移除
  replacer[frame_id % max_size] = State::EMPTY;
}

//取消对帧的锁定
void ClockReplacer::Unpin(frame_id_t frame_id) 
{
  //添加到替换器中
  replacer[frame_id % max_size] = State::ACCESSED;
}

/**
 * @brief 计算那些状态不等于EMPTY的数量
 * @return 返回替换器的当前大小
 */
size_t ClockReplacer::Size() 
{
  return count_if(replacer.begin(), replacer.end(), IsEmpty);
}

/**
 * @param item
 * @return 如果item的状态等于State::EMPTY，返回true，否则返回false
 */
bool ClockReplacer::IsEmpty(ClockReplacer::State& item) 
{
  return item == State::EMPTY;
}
