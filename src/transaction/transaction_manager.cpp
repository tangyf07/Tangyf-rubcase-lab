/* Copyright (c) 2023 Renmin University of China */
#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"
#include <cstring>

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        txn_id_t txn_id = next_txn_id_++;
        txn = new Transaction(txn_id);
        if (concurrency_mode_ == ConcurrencyMode::TWO_PHASE_LOCKING) {
            txn->set_state(TransactionState::GROWING);
        } else {
            txn->set_start_ts(next_timestamp_++);
            txn->set_state(TransactionState::DEFAULT);
        }
    }
    std::unique_lock<std::mutex> lock(latch_);
    txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (lock_manager_ != nullptr) {
        auto lock_set = *txn->get_lock_set();
        for (const auto& lock_id : lock_set) {
            lock_manager_->unlock(txn, lock_id);
        }
    }
    txn->set_state(TransactionState::COMMITTED);
}

void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    auto write_set = txn->get_write_set();

    // 【修改点】使用索引反向遍历，避免迭代器失效问题
    // 只要回滚进入 Abort 之前存在的日志，Abort 过程中产生的新日志忽略
    int size = write_set->size();
    for (int i = size - 1; i >= 0; i--) {
        auto wr = (*write_set)[i]; // 使用下标访问
        
        auto *file_handle = sm_manager_->fhs_.at(wr->GetTableName()).get();
        auto tab_meta = sm_manager_->db_.get_table(wr->GetTableName());

        // 1. INSERT 回滚
        if (wr->GetWriteType() == WType::INSERT_TUPLE) {
            // 先删索引
            for (auto& index : tab_meta.indexes) {
                std::vector<std::string> col_names;
                for(const auto& col : index.cols) col_names.push_back(col.name);
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(wr->GetTableName(), col_names);
                if (sm_manager_->ihs_.find(index_name) == sm_manager_->ihs_.end()) continue;
                auto ih = sm_manager_->ihs_.at(index_name).get();
                
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for (auto& col : index.cols) {
                    memcpy(key + offset, wr->GetRecord().data + col.offset, col.len);
                    offset += col.len;
                }
                ih->delete_entry(key, txn);
                delete[] key;
            }
            // 再删数据
            file_handle->delete_record(wr->GetRid(), nullptr);
        }
        
        // 2. DELETE 回滚
        else if (wr->GetWriteType() == WType::DELETE_TUPLE) {
            // 先插数据
            file_handle->insert_record(wr->GetRid(), wr->GetRecord().data);
            
            // 再插索引
            for (auto& index : tab_meta.indexes) {
                std::vector<std::string> col_names;
                for(const auto& col : index.cols) col_names.push_back(col.name);
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(wr->GetTableName(), col_names);
                if (sm_manager_->ihs_.find(index_name) == sm_manager_->ihs_.end()) continue;
                auto ih = sm_manager_->ihs_.at(index_name).get();
                
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for (auto& col : index.cols) {
                    memcpy(key + offset, wr->GetRecord().data + col.offset, col.len);
                    offset += col.len;
                }
                ih->insert_entry(key, wr->GetRid(), txn);
                delete[] key;
            }
        }
        
        // 3. UPDATE 回滚
        else if (wr->GetWriteType() == WType::UPDATE_TUPLE) {
            // 3.1 删新索引 (需要读当前表中的新值)
            auto new_record = file_handle->get_record(wr->GetRid(), nullptr);
            for (auto& index : tab_meta.indexes) {
                std::vector<std::string> col_names;
                for(const auto& col : index.cols) col_names.push_back(col.name);
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(wr->GetTableName(), col_names);
                if (sm_manager_->ihs_.find(index_name) == sm_manager_->ihs_.end()) continue;
                auto ih = sm_manager_->ihs_.at(index_name).get();

                char* new_key = new char[index.col_tot_len];
                int offset = 0;
                for (auto& col : index.cols) {
                    memcpy(new_key + offset, new_record->data + col.offset, col.len);
                    offset += col.len;
                }
                ih->delete_entry(new_key, txn);
                delete[] new_key;
            }
            
            // 【注意】这里 new_record 应该已经被销毁了，如果 get_record 返回的是 unique_ptr，那没问题。
            // 如果返回的是 raw pointer，你需要手动 delete new_record; 避免内存泄漏。

            // 3.2 恢复旧数据
            file_handle->update_record(wr->GetRid(), wr->GetRecord().data, nullptr);

            // 3.3 插旧索引
            for (auto& index : tab_meta.indexes) {
                std::vector<std::string> col_names;
                for(const auto& col : index.cols) col_names.push_back(col.name);
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(wr->GetTableName(), col_names);
                if (sm_manager_->ihs_.find(index_name) == sm_manager_->ihs_.end()) continue;
                auto ih = sm_manager_->ihs_.at(index_name).get();
                
                char* old_key = new char[index.col_tot_len];
                int offset = 0;
                for (auto& col : index.cols) {
                    memcpy(old_key + offset, wr->GetRecord().data + col.offset, col.len);
                    offset += col.len;
                }
                ih->insert_entry(old_key, wr->GetRid(), txn);
                delete[] old_key;
            }
        }
    }

    // 释放锁并修改状态
    if (lock_manager_ != nullptr) {
        // 复制锁集合，避免遍历时被修改 (Good practice)
        auto lock_set = *txn->get_lock_set();
        for (const auto& lock_id : lock_set) {
            lock_manager_->unlock(txn, lock_id);
        }
    }
    txn->set_state(TransactionState::ABORTED);
}