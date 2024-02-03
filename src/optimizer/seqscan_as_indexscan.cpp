#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  if (plan->GetType() != PlanType::SeqScan) {
    return plan;
  }
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    const auto filter_predicate = seq_scan_plan.filter_predicate_;
    if (filter_predicate != nullptr && filter_predicate->GetReturnType() == TypeId::BOOLEAN) {
      const auto comp_expr = dynamic_cast<const ComparisonExpression *>(filter_predicate.get());
      if (comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal && comp_expr->GetChildren().size() == 2) {
        const auto left_expr = comp_expr->GetChildAt(0);
        const auto right_expr = comp_expr->GetChildAt(1);
        const auto column_val = dynamic_cast<const ColumnValueExpression *>(left_expr.get());
        const auto indexes = catalog_.GetTableIndexes(catalog_.GetTable(seq_scan_plan.GetTableOid())->name_);

        const std::vector<uint32_t> filter_cols = { column_val->GetColIdx() };
        for (const auto &index : indexes) {
          const auto index_cols = index->index_->GetKeyAttrs();
          if (filter_cols == index_cols) {
            return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, seq_scan_plan.GetTableOid(), index->index_oid_, filter_predicate, nullptr);
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
