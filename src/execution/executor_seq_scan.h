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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     */
    void beginTuple() override {
        // ===== 任务3: 添加表 S 锁 =====
        if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
            int fd = fh_->GetFd();
            if (!context_->lock_mgr_->lock_shared_on_table(context_->txn_, fd)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
        // ==============================

        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(rec.get(), fed_conds_, cols_)) {
                return;
            }
            scan_->next();
        }
        rid_ = Rid{-1, -1};
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     */
    void nextTuple() override {
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(rec.get(), fed_conds_, cols_)) {
                return;
            }
            scan_->next();
        }
        rid_ = Rid{-1, -1};
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) return nullptr;
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }

    bool is_end() const override {
        return rid_.page_no == -1;
    }
    
    std::string getType() override { return "SeqScanExecutor"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

private:
    bool eval_cond(const RmRecord *rec, const Condition &cond,
                   const std::vector<ColMeta> &rec_cols) {
        char *lhs_val = nullptr;
        char *rhs_val = nullptr;
        ColType lhs_type, rhs_type;
        int lhs_len = 0, rhs_len = 0;

        auto lhs_it = get_col(rec_cols, cond.lhs_col);
        lhs_type = lhs_it->type;
        lhs_len  = lhs_it->len;
        lhs_val  = const_cast<char *>(rec->data + lhs_it->offset);

        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs_len  = cond.rhs_val.raw->size;
            rhs_val  = const_cast<char *>(cond.rhs_val.raw->data);
        } else {
            auto rhs_it = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_it->type;
            rhs_len  = rhs_it->len;
            rhs_val  = const_cast<char *>(rec->data + rhs_it->offset);
        }

        if (lhs_type != rhs_type) return false;

        int cmp = ix_compare(lhs_val, rhs_val, lhs_type, lhs_len);
        switch (cond.op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp <  0;
            case OP_GT: return cmp >  0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
            default:    return false;
        }
    }

    bool eval_conds(const RmRecord *rec, const std::vector<Condition> &conds, const std::vector<ColMeta> &rec_cols) {
        return std::all_of(conds.begin(), conds.end(),
            [&](const Condition &cond) { return eval_cond(rec, cond, rec_cols); }
        );
    }
};
