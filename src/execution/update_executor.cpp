//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/update_executor.h"
#include "storage/index/index.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->table_oid_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  int32_t n_updates = 0;

  Tuple child_tuple{};
  RID child_rid;

  std::vector<IndexInfo*> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
// Currently working on update executor
  while(child_executor_->Next(&child_tuple, &child_rid)) {

    std::vector<Value> updates;
    updates.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      updates.emplace_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    auto to_update_tuple = Tuple{updates, &child_executor_->GetOutputSchema()};

    TupleMeta meta{.is_deleted_ = false, .ts_ = 0};
    bool is_update_successful = table_info_->table_->UpdateTupleInPlace(meta, to_update_tuple, child_rid);
    if (is_update_successful) {
      for (auto index : indexes) {
        Tuple key = child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key, child_rid, exec_ctx_->GetTransaction());
        index->index_->InsertEntry(to_update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), child_rid, exec_ctx_->GetTransaction());
      }

      n_updates++;
    }
  }

  std::vector<Value> ret;
  ret.reserve(GetOutputSchema().GetColumnCount());
  ret.emplace_back(INTEGER, n_updates);
  *tuple = Tuple{ret, &GetOutputSchema()};

  return n_updates > 0;
}

}  // namespace bustub
