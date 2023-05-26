#include "buffer/clock_replacer.h"

ClockReplacer::ClockReplacer(size_t num_pages)
    :second_chance(num_pages,State::EMPTY),
      pointer(0),
      capacity(num_pages) 
      {
          
      }

ClockReplacer::~ClockReplacer() 
{
    
}

bool ClockReplacer::Victim(frame_id_t *frame_id) 
{
  size_t nonempty_count = 0, i;
  frame_id_t victim_frame_id = 0;

  for (i = 0; i < capacity; i++) 
  {
    auto id = (pointer + i) % capacity;
    if (second_chance[id] == State::EMPTY)
      continue;
    else if (second_chance[id] == State::ACCESSED) 
    {
      nonempty_count++;
      second_chance[id] = State::UNUSED; 
    } 
    else if (second_chance[id] == State::UNUSED) 
    {
      nonempty_count++;
      victim_frame_id = (victim_frame_id != 0) ? victim_frame_id : id;
    }
  }

  if (nonempty_count == 0) 
  {
    frame_id = nullptr;
    return false;
  }

  if (victim_frame_id == 0) 
  {
    for (i = 0; i < capacity; i++) 
    {
      auto id = (pointer + i) % capacity;
      if (second_chance[id] == State::UNUSED) 
      {
        victim_frame_id = id;
        break;
      }
    }
  }

  second_chance[victim_frame_id] = State::EMPTY;
  pointer = victim_frame_id;
  *frame_id = victim_frame_id;

  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) 
{
  second_chance[frame_id % capacity] = State::EMPTY;
}

void ClockReplacer::Unpin(frame_id_t frame_id) 
{
  second_chance[frame_id % capacity] = State::ACCESSED;
}

size_t ClockReplacer::Size() 
{
  return count_if(second_chance.begin(), second_chance.end(), IsEmpty);
}

bool ClockReplacer::IsEmpty(ClockReplacer::State& item) 
{
  return item != State::EMPTY;
}
