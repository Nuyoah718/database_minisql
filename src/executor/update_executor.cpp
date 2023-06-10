//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  return false;
}

// ** 模块五请注意，因为Row.h中不允许使用默认的无参构造函数！！
//因此需要使用Row类中的另一个构造函数Row(std::vector<Field> &fields)来创建一个Row实例
//这里面给出了一个例子，即：需要先创建一个std::vector<Field>实例，然后用它来构造Row实例
//若需在GenerateUpdatedTuple函数中创建一个从src_row复制的Row实例，则可使用Row类的拷贝构造函数
//在任何情况下，如果需要返回一个Row实例，都不能直接返回Row()，因为这个无参构造函数已被禁用
/*可以使用如下的形式：
Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) { return Row(src_row); } */
Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  std::vector<Field> fields;
  // Fill fields with the necessary data
  return Row(fields);
}
