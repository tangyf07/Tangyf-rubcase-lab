/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2  
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

    // 当前左右元组缓存（避免重复调用Next）
    std::unique_ptr<RmRecord> left_rec_;
    std::unique_ptr<RmRecord> right_rec_;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
    }

    void beginTuple() override {
    left_->beginTuple();
    right_->beginTuple();
    isend = left_->is_end();

    if (!isend && !right_->is_end()) {
        left_rec_ = left_->Next();
        right_rec_ = right_->Next();
    }
}

   void nextTuple() override {
    if (isend) return;

    while (true) {
        // 移动右表
        right_->nextTuple();
        if (!right_->is_end()) {
            right_rec_ = right_->Next();
        } else {
            // 右表走完，移动左表，重置右表
            left_->nextTuple();
            if (left_->is_end()) {
                isend = true;
                return;
            }
            right_->beginTuple();
            if (right_->is_end()) {
                isend = true;
                return;
            }
            right_rec_ = right_->Next();
            left_rec_ = left_->Next();
        }

        // 检查条件
        if (eval_conds(left_rec_.get(), right_rec_.get(), fed_conds_, cols_)) {
            break;
        }
    }
}

   std::unique_ptr<RmRecord> Next() override {
    auto rec = std::make_unique<RmRecord>(len_);
    memcpy(rec->data, left_rec_->data, left_->tupleLen());
    memcpy(rec->data + left_->tupleLen(), right_rec_->data, right_->tupleLen());
    return rec;
}

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return isend; }
    
    std::string getType() override { return "NestedLoopJoinExecutor"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    private:
    bool eval_cond(const RmRecord *lhs_rec, const RmRecord *rhs_rec, const Condition &cond,
               const std::vector<ColMeta> &rec_cols) {
    const auto &left_cols = left_->cols();
    const auto &right_cols = right_->cols();

    auto lhs_col_it = get_col(left_cols, cond.lhs_col);
    auto rhs_col_it = get_col(right_cols, cond.rhs_col);

    const char *lhs_val = lhs_rec->data + lhs_col_it->offset;
    const char *rhs_val = rhs_rec->data + rhs_col_it->offset;

    int cmp = ix_compare(lhs_val, rhs_val, lhs_col_it->type, lhs_col_it->len);

    switch (cond.op) {
        case OP_EQ: return cmp == 0;
        case OP_NE: return cmp != 0;
        case OP_LT: return cmp < 0;
        case OP_LE: return cmp <= 0;
        case OP_GT: return cmp > 0;
        case OP_GE: return cmp >= 0;
        default: return false;
    }
}

    bool eval_conds(const RmRecord *lhs_rec, const RmRecord *rhs_rec, const std::vector<Condition> &conds,
                const std::vector<ColMeta> &rec_cols) {
        if (conds.empty()) return true;  // ← 加这一行
        return std::all_of(conds.begin(), conds.end(),
            [&](const Condition &cond) { return eval_cond(lhs_rec, rhs_rec, cond, rec_cols); }
        );
    }
};