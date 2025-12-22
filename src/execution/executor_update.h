#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
               std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;

        for (auto &set_clause : set_clauses_) {
            set_clause.lhs.tab_name = tab_name_;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        for (auto &rid : rids_) {
            auto old_rec = fh_->get_record(rid, context_);

            // 记录写操作
            if (context_->txn_ != nullptr) {
                auto wr = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *old_rec);
                context_->txn_->append_write_record(wr);
            }

            RmRecord new_rec(old_rec->size);
            memcpy(new_rec.data, old_rec->data, old_rec->size);

            for (auto &set_clause : set_clauses_) {
                auto col_pos = get_col(tab_.cols, set_clause.lhs);
                auto &col = *col_pos;
                auto &val = set_clause.rhs;
                if (!val.raw) val.init_raw(col.len);
                memcpy(new_rec.data + col.offset, val.raw->data, col.len);
            }

            // 更新数据
            fh_->update_record(rid, new_rec.data, context_);

            // 更新索引
            for (auto &index : tab_.indexes) {
                auto &cols = index.cols;
                bool index_changed = false;
                for (auto &set_clause : set_clauses_) {
                    if (std::find_if(cols.begin(), cols.end(),
                        [&](const ColMeta &c) { return c.name == set_clause.lhs.col_name; }) != cols.end()) {
                        index_changed = true;
                        break;
                    }
                }
                if (!index_changed) continue;

                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, cols)).get();

                char *old_key = new char[index.col_tot_len];
                int offset = 0;
                for (auto &c : cols) {
                    memcpy(old_key + offset, old_rec->data + c.offset, c.len);
                    offset += c.len;
                }
                ih->delete_entry(old_key, context_->txn_);

                char *new_key = new char[index.col_tot_len];
                offset = 0;
                for (auto &c : cols) {
                    memcpy(new_key + offset, new_rec.data + c.offset, c.len);
                    offset += c.len;
                }
                ih->insert_entry(new_key, rid, context_->txn_);

                delete[] old_key;
                delete[] new_key;
            }
        }
        return nullptr;
    }
    Rid &rid() override { return _abstract_rid; }
};