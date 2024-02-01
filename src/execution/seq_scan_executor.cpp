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
}

void SeqScanExecutor::Init() {
    table_oid_t table_oid = plan_->GetTableOid();
    Catalog* catalog = exec_ctx_->GetCatalog();
    TableInfo* table_info = catalog->GetTable(table_oid);
    table_heap_ = table_info->table_.get();
    TableIterator table_itr = table_heap_->MakeIterator();
    // get table metadata?
    // initialize objects to read from table
    // initialize counter 

    // throw NotImplementedException("SeqScanExecutor is not implemented"); 
    while (!table_itr.IsEnd()) {
        if (table_itr.GetTuple().first.is_deleted_) {
           ++table_itr;
           continue;
        }
        rids_.push_back(table_itr.GetRID());
        ++table_itr;
    }

    rids_iter_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {  

    // currently working
    if (rids_iter_ == rids_.end()) {
        return false;
    }

    *tuple = table_heap_->GetTuple(*rids_iter_).second;
    *rid = *rids_iter_;

    Schema schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;

    // std::cout << tuple->ToString(&schema) << ' ' << schema.ToString() << std::endl;

    rids_iter_++;
    return true;
}

}  // namespace bustub
