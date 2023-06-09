#ifndef MINISQL_MEM_HEAP_H
#define MINISQL_MEM_HEAP_H

#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include "common/macros.h"

// MemHeap 类定义了内存分配器的基本接口，
// SimpleMemHeap 是 MemHeap 的一个简单实现，
// 它使用 std::unordered_set 进行分配内存的记录。

class MemHeap {
 public:
  virtual ~MemHeap() = default;

  // 分配指定大小的内存，返回一个指向分配内存的指针。
  virtual void *Allocate(size_t size) = 0;

  // 释放指定的内存。
  virtual void Free(void *ptr) = 0;
};

class SimpleMemHeap : public MemHeap {
 public:
  ~SimpleMemHeap() {
    for (auto allocated_ptr: allocated_memory_set_) {
      Free(allocated_ptr);
    }
  }

  void *Allocate(size_t size) override {
    void *buffer = malloc(size);
    ASSERT(buffer != nullptr, "Out of memory exception");
    allocated_memory_set_.insert(buffer);
    return buffer;
  }

  void Free(void *ptr) override {
    if (ptr == nullptr) {
      return;
    }
    auto iter = allocated_memory_set_.find(ptr);
    if (iter != allocated_memory_set_.end()) {
      free(*iter);
      allocated_memory_set_.erase(iter);
    }
  }

 private:
  std::unordered_set<void *> allocated_memory_set_;
};

#endif //MINISQL_MEM_HEAP_H
