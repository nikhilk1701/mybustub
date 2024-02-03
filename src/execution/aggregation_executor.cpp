//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), aht_(SimpleAggregationHashTable{plan->aggregates_, plan->agg_types_}), aht_iterator_(aht_.Begin()) {
        // construct aht_
            // fetch agg_types
            // fetch agg_exprs
        // generate aht_iterator_        
    }

void AggregationExecutor::Init() {
    child_executor_->Init();
    Tuple tuple;
    RID rid;

    while (child_executor_->Next(&tuple, &rid)) {
        // extract cols required for aggregation
        // extract group_by cols
        std::vector<Value> group_bys;
        std::vector<Value> curr_val;
        group_bys.reserve(plan_->GetGroupBys().size());
        curr_val.reserve(plan_->aggregates_.size());
        for (const auto &group_by_expr : plan_->GetGroupBys()) {
            group_bys.emplace_back(group_by_expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
        }
        for (const auto &agg_expr : plan_->aggregates_) {
            curr_val.emplace_back(agg_expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
        }
        const AggregateKey &agg_key = AggregateKey{.group_bys_ = group_bys};
        const AggregateValue &agg_val = AggregateValue{.aggregates_ = curr_val};
        std::cout << "calling insert combine: ";
        for (const auto &x : agg_key.group_bys_) { 
            std::cout << x.ToString() << ", ";
        }
        for (const auto &x : agg_val.aggregates_) {  
            std::cout << x.ToString() << ", "; 
        } std::cout << std::endl;
        aht_.InsertCombine(agg_key, agg_val);
    }
    if (aht_.Begin() == aht_.End() && GetOutputSchema().GetColumnCount() == 1) {
        aht_.InsertEmptyCombine();
    }

    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (aht_iterator_ == aht_.End()) {
        return false;
    }
    std::vector<Value> key = aht_iterator_.Key().group_bys_;
    std::vector<Value> val = aht_iterator_.Val().aggregates_;
    Schema agg_schema = AggregationPlanNode::InferAggSchema(plan_->GetGroupBys(), plan_->GetAggregates(), plan_->GetAggregateTypes());
    key.insert(key.end(), val.begin(), val.end());
    *tuple = Tuple{key, &agg_schema};
    ++aht_iterator_;
    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
