//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
    plan_ = plan;
    child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() { 
    child_executor_->Init();
    // std::unique_ptr<TableHeap> table_heap = std::move(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_);
    table_oid_t table_oid = plan_->GetTableOid();
    Catalog* catalog = exec_ctx_->GetCatalog();
    TableInfo* table_info = catalog->GetTable(table_oid);
    table_heap_ = table_info->table_.get();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    int32_t n_insert = 0;

    Catalog* catalog = exec_ctx_->GetCatalog();
    TableInfo* table_info = catalog->GetTable(plan_->GetTableOid());
    std::string table_name = table_info->name_;
    std::vector<IndexInfo*> indexes = catalog->GetTableIndexes(table_name);

    Tuple child_tuple{};
    RID child_rid{};

    while (child_executor_->Next(&child_tuple, &child_rid)) {
        auto meta = TupleMeta{.is_deleted_ =  false, .ts_ =  0};   
        std::optional<RID> rid = table_heap_->InsertTuple(meta, child_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->GetTableOid());
        if (rid.has_value()) {
            // update tables indexes
            for (auto index : indexes) {
                Schema key_schema = index->key_schema_;
                Tuple key = child_tuple.KeyFromTuple(table_info->schema_, key_schema, index->index_->GetKeyAttrs());
                index->index_->InsertEntry(key, rid.value(), exec_ctx_->GetTransaction());
            }
            n_insert++;
        }
    }

    std::vector<Value> ret;
    ret.reserve(GetOutputSchema().GetColumnCount());
    ret.emplace_back(INTEGER, n_insert);
    *tuple = Tuple{ret, &GetOutputSchema()};
    std::cout << tuple->ToString(&GetOutputSchema()) << std::endl;
    
    if (!called_ && n_insert == 0) {
        called_ = true;
        return true;
    }
    called_ = true;
    return n_insert > 0;
}
}  // namespace bustub
