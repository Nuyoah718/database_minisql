/*
* 文件名: mem_heap.h
*
* 描述: 这个文件定义了两个内存分配器的类，MemHeap 和 SimpleMemHeap。
* MemHeap 是一个抽象类，定义了内存分配器的基本接口。SimpleMemHeap 是 MemHeap 的一个简单实现，
* 它使用 std::unordered_set 来保存分配的内存，以便在对象销毁时释放所有分配的内存。
*
* Author: Weilin Chang
* Last Modify Date: 6/9/2023
*/

#ifndef MINISQL_MEM_HEAP_H
#define MINISQL_MEM_HEAP_H

#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include "common/macros.h"

// 内存分配器的抽象基类。
class MemHeap {
public:
 virtual ~MemHeap() = default;

 // 分配指定大小的内存，返回一个指向分配内存的指针。
 virtual void *Allocate(size_t size) = 0;

 // 释放指定的内存。
 virtual void Free(void *ptr) = 0;
};

// MemHeap 的一个简单实现，使用 std::unordered_set 进行分配内存的记录。
class SimpleMemHeap : public MemHeap {
public:
 // 在析构函数中，释放所有分配的内存。
 ~SimpleMemHeap() override {
   ClearAllocatedMemory();
 }

 // 分配指定大小的内存，将内存指针保存在 unordered_set 中，并返回内存指针。
 void *Allocate(size_t size) override {
   void *buffer = std::malloc(size);
   ASSERT(buffer != nullptr, "Out of memory exception");
   allocated_memory_set_.insert(buffer);
   return buffer;
 }

 // 释放指定的内存，并从 unordered_set 中移除对应的内存指针。
 void Free(void *ptr) override {
   if (ptr == nullptr) {
     return;
   }
   FreeAllocatedMemory(ptr);
 }

private:
 // 保存已分配内存的指针的集合。
 std::unordered_set<void *> allocated_memory_set_;

 // 清空并释放所有分配的内存。
 void ClearAllocatedMemory() {
   for (auto allocated_ptr : allocated_memory_set_) {
     std::free(allocated_ptr);
   }
   allocated_memory_set_.clear();
 }

 // 释放指定的内存，并从 allocated_memory_set_ 中移除该内存的指针。
 void FreeAllocatedMemory(void *ptr) {
   auto iter = allocated_memory_set_.find(ptr);
   if (iter != allocated_memory_set_.end()) {
     std::free(*iter);
     allocated_memory_set_.erase(iter);
   }
 }
};

#endif //MINISQL_MEM_HEAP_H
