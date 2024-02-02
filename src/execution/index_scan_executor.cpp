//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "catalog/catalog.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/index/index.h"
#include "storage/table/tuple.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
    std::cout << plan_->ToString(true) << std::endl;
    std::cout << plan_->filter_predicate_->ToString() << std::endl;

    table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
    index_oid_t index_oid = plan_->GetIndexOid();
    index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_oid);
    auto hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
    std::cout << plan_->filter_predicate_->ToString() << std::endl;
    rids_.clear();
    if (plan_->filter_predicate_ != nullptr) {
        auto right_expr = std::dynamic_pointer_cast<ConstantValueExpression>(plan_->filter_predicate_->GetChildAt(1));
        Tuple key{{right_expr->val_}, index_info_->index_->GetKeySchema()};
        hash_index->ScanKey(key, &rids_, exec_ctx_->GetTransaction());
    }
    rids_iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    std::pair<TupleMeta, Tuple> tuple_info;
    do {
        if (rids_iter_ == rids_.end()) {
            return false;;
        }
        
        tuple_info = table_info_->table_->GetTuple(*rids_iter_);
        *tuple = tuple_info.second;
        *rid = *rids_iter_;
        rids_iter_++;
    } while (tuple_info.first.is_deleted_);
    
    return true;
}
} // namespace bustub