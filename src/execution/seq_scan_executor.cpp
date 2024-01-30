//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "recovery/log_manager.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) { 
    plan_ = plan;
    table_itr_ =  nullptr;
}

void SeqScanExecutor::Init() {
    table_oid_t table_oid = plan_->GetTableOid();
    Catalog* catalog = exec_ctx_->GetCatalog();
    TableInfo* table_info = catalog->GetTable(table_oid);
    TableHeap* table_heap = table_info->table_.get();
    TableIterator table_itr = table_heap->MakeIterator();
    table_itr_ = &table_itr;
    // get table metadata?
    // initialize objects to read from table
    // initialize counter 

    // throw NotImplementedException("SeqScanExecutor is not implemented"); 
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {  

    // currently working
    if (table_itr_->IsEnd()) {
        return false;
    }

    TupleMeta tuple_mtd = table_itr_->GetTuple().first;
    if (tuple_mtd.is_deleted_) {
        return false;
    }
    *tuple = table_itr_->GetTuple().second;
    *rid = table_itr_->GetRID();

    Schema schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;

    if (plan_->filter_predicate_->Evaluate(tuple, schema).IsNull()) {
        return false;
    }

    table_itr_++;
    return true;
}

}  // namespace bustub
