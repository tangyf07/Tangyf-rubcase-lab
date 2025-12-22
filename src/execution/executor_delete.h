#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (rids_.empty()) return nullptr;

        for (const Rid &rid : rids_) {
            auto oldRec = fh_->get_record(rid, context_);

            // 记录写操作
            if (context_->txn_ != nullptr) {
                auto wr = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *oldRec);
                context_->txn_->append_write_record(wr);
            }

            // 删除索引
            for (auto &idx : tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(
                    sm_manager_->get_ix_manager()->get_index_name(tab_name_, idx.cols)).get();

                char* key = new char[idx.col_tot_len];
                int off = 0;
                for (int j = 0; j < idx.col_num; ++j) {
                    const auto &c = idx.cols[j];
                    std::memcpy(key + off, oldRec->data + c.offset, c.len);
                    off += c.len;
                }
                ih->delete_entry(key, context_->txn_);
                delete[] key;
            }

            // 删除数据
            fh_->delete_record(rid, context_);
        }
        rids_.clear();
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};