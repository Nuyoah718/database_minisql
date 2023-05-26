#include "buffer/clock_replacer.h"

ClockReplacer::ClockReplacer(size_t num_pages)
    : m_states(num_pages, State::EMPTY),
      m_pointer(0),
      m_capacity(num_pages)
{}

ClockReplacer::~ClockReplacer() {}

bool ClockReplacer::pickVictim(frame_id_t* frame_id) {
  size_t non_empty_count = 0;
  frame_id_t victim_frame_id = 0;

  for (size_t i = 0; i < m_capacity; i++) {
    const auto id = (m_pointer + i) % m_capacity;
    if (m_states[id] == State::EMPTY) {
      continue;
    } else if (m_states[id] == State::ACCESSED) {
      non_empty_count++;
      m_states[id] = State::UNUSED;
    } else if (m_states[id] == State::UNUSED) {
      non_empty_count++;
      victim_frame_id = (victim_frame_id != 0) ? victim_frame_id : id;
    }
  }

  if (non_empty_count == 0) {
    *frame_id = 0;
    return false;
 }

  if (victim_frame_id == 0) {
    for (size_t i = 0; i < m_capacity; i++) {
      const auto id = (m_pointer + i) % m_capacity;
      if (m_states[id] == State::UNUSED) {
        victim_frame_id = id;
        break;
      }
    }
  }

  m_states[victim_frame_id] = State::EMPTY;
  m_pointer = victim_frame_id;
  *frame_id = victim_frame_id;

  return true;
}

void ClockReplacer::pin(frame_id_t frame_id) {
  m_states[frame_id % m_capacity] = State::EMPTY;
}

void ClockReplacer::unpin(frame_id_t frame_id) {
  m_states[frame_id % m_capacity] = State::ACCESSED;
}

size_t ClockReplacer::size() const {
  return std::count_if(m_states.begin(), m_states.end(), IsNotEmpty);
}

bool ClockReplacer::IsNotEmpty(const ClockReplacer::State& item) {
  return item != State::EMPTY;
}
