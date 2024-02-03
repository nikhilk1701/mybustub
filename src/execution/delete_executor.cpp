//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) { }

void DeleteExecutor::Init() { 
    child_executor_->Init();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    if (called_) {
        return false;
    }

    int32_t n_deletes = 0;

    Tuple child_tuple{};
    RID child_rid;

    std::vector<IndexInfo*> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

    while (child_executor_->Next(&child_tuple, &child_rid)) {
        TupleMeta meta = table_info_->table_->GetTupleMeta(child_rid);
        if (meta.is_deleted_) {
            continue;
        }
        meta.is_deleted_ = true;
        table_info_->table_->UpdateTupleMeta(meta, child_rid);

        for (auto index : indexes) {
            Tuple key = child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
            index->index_->DeleteEntry(key, child_rid, exec_ctx_->GetTransaction());
        }

        n_deletes++;
    }

    std::vector<Value> ret;
    ret.reserve(GetOutputSchema().GetColumnCount());
    ret.emplace_back(INTEGER, n_deletes);
    *tuple = Tuple{ret, &GetOutputSchema()};
    called_ = true;
    return true;
}
} // namespace bustub
