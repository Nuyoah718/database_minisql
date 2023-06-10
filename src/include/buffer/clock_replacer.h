#ifndef MINISQL_CLOCK_REPLACER_H
#define MINISQL_CLOCK_REPLACER_H

#include <algorithm>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * CLOCKReplacer implements the clock replacement.
 */
class CLOCKReplacer : public Replacer {
 public:
  /**
   * Create a new CLOCKReplacer.
   * @param num_pages the maximum number of pages the CLOCKReplacer will be required to store
   */
  explicit CLOCKReplacer(size_t num_pages);

  /**
   * Destroys the CLOCKReplacer.
   */
  ~CLOCKReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  enum class State { EMPTY, ACCESSED, UNUSED };
  static bool IsEmpty(CLOCKReplacer::State &);

  size_t capacity;
  list<frame_id_t> clock_list;               // replacer中可以被替换的数据页
  map<frame_id_t, frame_id_t> clock_status;  // 数据页的存储状态
  //使用map<frame_id_t, bool>表示clock_status，
  //其中key是frame_id，value表示该帧的访问状态，如果访问过则为true，否则为false
  std::vector<State> second_chance;
  frame_id_t pointer{0};
};

#endif  // MINISQL_CLOCK_REPLACER_H
