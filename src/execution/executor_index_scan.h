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

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<IxScan> scan_;              // 索引扫描器
    IxIndexHandle *ih_;                         // 索引句柄

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, 
                      std::vector<std::string> index_col_names, Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        
        // 获取索引句柄
        std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_);
        ih_ = sm_manager_->ihs_.at(index_name).get();
        
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, 
            {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        // ===== 任务3: 添加表 S 锁 =====
        if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
            int fd = fh_->GetFd();
            if (!context_->lock_mgr_->lock_shared_on_table(context_->txn_, fd)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
        // ==============================

        // 构建索引扫描的 key
        char *key = new char[index_meta_.col_tot_len];
        memset(key, 0, index_meta_.col_tot_len);
        
        int offset = 0;
        for (size_t i = 0; i < index_meta_.col_num; i++) {
            auto &idx_col = index_meta_.cols[i];
            for (auto &cond : fed_conds_) {
                if (cond.lhs_col.col_name == idx_col.name && 
                    cond.op == OP_EQ && cond.is_rhs_val) {
                    memcpy(key + offset, cond.rhs_val.raw->data, idx_col.len);
                    break;
                }
            }
            offset += idx_col.len;
        }
        
        Iid lower = ih_->lower_bound(key);
        Iid upper = ih_->upper_bound(key);
        
        delete[] key;
        
        scan_ = std::make_unique<IxScan>(ih_, lower, upper, sm_manager_->get_bpm());
        
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                break;
            }
            scan_->next();
        }
    }
    
    void nextTuple() override {
        if (scan_->is_end()) return;
        
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                break;
            }
            scan_->next();
        }
    }
    
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) return nullptr;
        return fh_->get_record(rid_, context_);
    }
    
    Rid &rid() override { return rid_; }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }
    
    size_t tupleLen() const override {
        return len_;
    }
    
    std::string getType() override {
        return "IndexScanExecutor";
    }
    
    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

private:
    int compare_value(const char *a, const char *b, ColType type, int len) {
        switch (type) {
            case TYPE_INT: {
                int ai = *(int *)a;
                int bi = *(int *)b;
                return (ai < bi) ? -1 : ((ai > bi) ? 1 : 0);
            }
            case TYPE_FLOAT: {
                float af = *(float *)a;
                float bf = *(float *)b;
                return (af < bf) ? -1 : ((af > bf) ? 1 : 0);
            }
            case TYPE_STRING:
                return memcmp(a, b, len);
            default:
                return 0;
        }
    }

    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec) {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs = rec->data + lhs_col->offset;
        char *rhs;
        ColType rhs_type;
        int rhs_len;
        
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
            rhs_len = lhs_col->len;
        } else {
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rec->data + rhs_col->offset;
            rhs_len = rhs_col->len;
        }
        
        int cmp = compare_value(lhs, rhs, rhs_type, rhs_len);
        
        switch (cond.op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
            default: return false;
        }
    }

    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec) {
        for (auto &cond : conds) {
            if (!eval_cond(rec_cols, cond, rec)) return false;
        }
        return true;
    }
};
