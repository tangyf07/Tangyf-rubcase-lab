#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction.h"
#include "common/context.h"
#include "transaction/txn_defs.h" // 确保包含 LockMode 定义

// -------------------------------------------------------------------------
// 辅助逻辑：冲突矩阵
// S  冲突: X, IX, SIX
// X  冲突: S, X, IS, IX, SIX (所有)
// IS 冲突: X
// IX 冲突: S, X, SIX
// SIX 冲突: IX, S, X, SIX (除了 IS)
// -------------------------------------------------------------------------

bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    auto &queue = lock_table_[lock_id];

    // 1. 重入检查
    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::SHARED || req.lock_mode_ == LockMode::EXLUCSIVE) {
                return true;
            }
        }
    }

    // 2. 冲突检查 (申请 S 锁)
    // S 锁与 X 锁冲突
    for (const auto& req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::EXLUCSIVE) {
                txn->set_state(TransactionState::ABORTED);
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    // 3. 申请锁
    LockRequest req(txn->get_transaction_id(), LockMode::SHARED);
    req.granted_ = true;
    queue.request_queue_.push_back(req);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    auto &queue = lock_table_[lock_id];

    // 1. 重入与升级检查
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            if (it->lock_mode_ == LockMode::EXLUCSIVE) return true;
            
            // S -> X 升级
            if (it->lock_mode_ == LockMode::SHARED) {
                // 检查是否有其他事务持有锁（必须独占）
                for (const auto& other : queue.request_queue_) {
                    if (other.granted_ && other.txn_id_ != txn->get_transaction_id()) {
                        txn->set_state(TransactionState::ABORTED);
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                    }
                }
                it->lock_mode_ = LockMode::EXLUCSIVE;
                return true;
            }
        }
    }

    // 2. 冲突检查 (申请 X 锁)
    // X 锁与任何锁都冲突
    for (const auto& req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
            txn->set_state(TransactionState::ABORTED);
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    // 3. 申请锁
    LockRequest req(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    req.granted_ = true;
    queue.request_queue_.push_back(req);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_id];

    // 1. 重入与升级
    for (auto &req : queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::SHARED || 
                req.lock_mode_ == LockMode::EXLUCSIVE ||
                req.lock_mode_ == LockMode::S_IX) return true;

            // IX -> SIX 升级
            if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
                for (const auto& other : queue.request_queue_) {
                    if (other.granted_ && other.txn_id_ != txn->get_transaction_id()) {
                        // SIX 与非 IS 锁冲突
                        if (other.lock_mode_ != LockMode::INTENTION_SHARED) {
                            txn->set_state(TransactionState::ABORTED);
                            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                        }
                    }
                }
                req.lock_mode_ = LockMode::S_IX;
                return true;
            }
            // IS -> S 升级
            if (req.lock_mode_ == LockMode::INTENTION_SHARED) {
                for (const auto& other : queue.request_queue_) {
                     if (other.granted_ && other.txn_id_ != txn->get_transaction_id()) {
                         // S 与 X, IX, SIX 冲突
                         if (other.lock_mode_ == LockMode::EXLUCSIVE ||
                             other.lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                             other.lock_mode_ == LockMode::S_IX) {
                             txn->set_state(TransactionState::ABORTED);
                             throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                         }
                     }
                }
                req.lock_mode_ = LockMode::SHARED;
                return true;
            }
        }
    }

    // 2. 冲突检查 (申请 S 锁)
    // S 与 X, IX, SIX 冲突
    for (const auto& req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::EXLUCSIVE || 
                req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE || 
                req.lock_mode_ == LockMode::S_IX) {
                txn->set_state(TransactionState::ABORTED);
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    // 3. 申请锁
    LockRequest req(txn->get_transaction_id(), LockMode::SHARED);
    req.granted_ = true;
    queue.request_queue_.push_back(req);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_id];

    // 1. 重入与升级
    for (auto &req : queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::EXLUCSIVE) return true;
            
            // 任何锁升级到 X (需要完全独占)
            for (const auto& other : queue.request_queue_) {
                if (other.granted_ && other.txn_id_ != txn->get_transaction_id()) {
                    txn->set_state(TransactionState::ABORTED);
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                }
            }
            req.lock_mode_ = LockMode::EXLUCSIVE;
            return true;
        }
    }

    // 2. 冲突检查 (申请 X 锁)
    // X 与所有锁冲突
    for (const auto& req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
            txn->set_state(TransactionState::ABORTED);
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    // 3. 申请锁
    LockRequest req(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    req.granted_ = true;
    queue.request_queue_.push_back(req);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_id];

    // 重入: 任何锁都兼容 IS
    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) return true;
    }

    // 冲突检查 (申请 IS)
    // IS 仅与 X 冲突
    for (const auto& req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::EXLUCSIVE) {
                txn->set_state(TransactionState::ABORTED);
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    LockRequest req(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    req.granted_ = true;
    queue.request_queue_.push_back(req);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    auto &queue = lock_table_[lock_id];

    // 1. 重入与升级
    for (auto &req : queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE || 
                req.lock_mode_ == LockMode::EXLUCSIVE ||
                req.lock_mode_ == LockMode::S_IX) return true;
            
            // S -> SIX
            if (req.lock_mode_ == LockMode::SHARED) {
                // SIX 冲突: 非 IS
                for (const auto& other : queue.request_queue_) {
                    if (other.granted_ && other.txn_id_ != txn->get_transaction_id()) {
                        if (other.lock_mode_ != LockMode::INTENTION_SHARED) {
                            txn->set_state(TransactionState::ABORTED);
                            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                        }
                    }
                }
                req.lock_mode_ = LockMode::S_IX;
                return true;
            }
            // IS -> IX
            if (req.lock_mode_ == LockMode::INTENTION_SHARED) {
                // IX 冲突: S, X, SIX
                for (const auto& other : queue.request_queue_) {
                    if (other.granted_ && other.txn_id_ != txn->get_transaction_id()) {
                        if (other.lock_mode_ == LockMode::SHARED || 
                            other.lock_mode_ == LockMode::EXLUCSIVE || 
                            other.lock_mode_ == LockMode::S_IX) {
                             txn->set_state(TransactionState::ABORTED);
                             throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
                        }
                    }
                }
                req.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
                return true;
            }
        }
    }

    // 2. 冲突检查 (申请 IX)
    // IX 冲突: S, X, SIX
    for (const auto& req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ != txn->get_transaction_id()) {
            if (req.lock_mode_ == LockMode::SHARED || 
                req.lock_mode_ == LockMode::EXLUCSIVE || 
                req.lock_mode_ == LockMode::S_IX) {
                txn->set_state(TransactionState::ABORTED);
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    LockRequest req(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    req.granted_ = true;
    queue.request_queue_.push_back(req);
    txn->get_lock_set()->insert(lock_id);
    return true;
}

bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);
    if (lock_table_.find(lock_data_id) == lock_table_.end()) return false;
    
    auto &queue = lock_table_[lock_data_id];
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            queue.request_queue_.erase(it);
            if (queue.request_queue_.empty()) {
                lock_table_.erase(lock_data_id);
            }
            return true;
        }
    }
    return false;
}