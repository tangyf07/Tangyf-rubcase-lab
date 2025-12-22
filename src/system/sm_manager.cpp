/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    std::ofstream ofs(DB_META_NAME);
    ofs << *new_db;

    delete new_db;

    disk_manager_->create_file(LOG_FILE_NAME);

    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库
 */
void SmManager::open_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    std::ifstream ifs(DB_META_NAME);
    if (!ifs.is_open()) {
        throw UnixError();
    }
    ifs >> db_;

    fhs_.clear();
    ihs_.clear();
    for (auto &entry : db_.tabs_) {
        const std::string &tab_name = entry.first;
        fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    flush_meta();

    for (auto itf = fhs_.begin(); itf != fhs_.end(); ++itf) {
        rm_manager_->close_file(itf->second.get());
    }

    fhs_.clear();
    ihs_.clear();

    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    printer.print_separator(context);
}

/**
 * @description: 创建表
 */
void SmManager::create_table(const std::string& tab_name,
                             const std::vector<ColDef>& col_defs,
                             Context* context) {

    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }

    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;

    for (auto &col_def : col_defs) {
        ColMeta col = {
            .tab_name = tab_name,
            .name = col_def.name,
            .type = col_def.type,
            .len = col_def.len,
            .offset = curr_offset,
            .index = false
        };
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }

    int record_size = curr_offset;

    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    // ===== 任务3: 添加 X 锁 =====
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        int fd = fhs_.at(tab_name)->GetFd();
        if (!context->lock_mgr_->lock_exclusive_on_table(context->txn_, fd)) {
            throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }
    // ============================

    flush_meta();
}

/**
 * @description: 删除表
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // ===== 任务3: 添加 X 锁 =====
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        auto itf = fhs_.find(tab_name);
        if (itf != fhs_.end()) {
            int fd = itf->second->GetFd();
            if (!context->lock_mgr_->lock_exclusive_on_table(context->txn_, fd)) {
                throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }
    // ============================

    auto itf = fhs_.find(tab_name);
    if (itf != fhs_.end()) {
        rm_manager_->close_file(itf->second.get());
        fhs_.erase(itf);
    }

    rm_manager_->destroy_file(tab_name);
    db_.tabs_.erase(tab_name);
    flush_meta();
}

/**
 * @description: 创建索引
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {

    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // ===== 任务3: 添加 S 锁 =====
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        auto itf = fhs_.find(tab_name);
        if (itf != fhs_.end()) {
            int fd = itf->second->GetFd();
            if (!context->lock_mgr_->lock_shared_on_table(context->txn_, fd)) {
                throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }
    // ============================
    
    TabMeta& tab = db_.get_table(tab_name);
    
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }
    
    std::vector<ColMeta> index_cols;
    int col_tot_len = 0;
    for (const auto& col_name : col_names) {
        auto col_it = tab.get_col(col_name);
        index_cols.push_back(*col_it);
        col_tot_len += col_it->len;
    }
    
    ix_manager_->create_index(tab_name, index_cols);
    auto ih = ix_manager_->open_index(tab_name, index_cols);
    
    IndexMeta index_meta;
    index_meta.tab_name = tab_name;
    index_meta.col_tot_len = col_tot_len;
    index_meta.col_num = col_names.size();
    index_meta.cols = index_cols;
    tab.indexes.push_back(index_meta);
    
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);
    ihs_[index_name] = std::move(ih);
    
    flush_meta();
}

/**
 * @description: 删除索引
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // ===== 任务3: 添加 S 锁 =====
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        auto itf = fhs_.find(tab_name);
        if (itf != fhs_.end()) {
            int fd = itf->second->GetFd();
            if (!context->lock_mgr_->lock_shared_on_table(context->txn_, fd)) {
                throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }
    // ============================

    TabMeta& tab = db_.get_table(tab_name);

    if (!tab.is_index(col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);

    auto it = ihs_.find(index_name);
    if (it != ihs_.end()) {
        ix_manager_->close_index(it->second.get());
        ihs_.erase(it);
    }

    std::vector<ColMeta> index_cols;
    for (const auto& col_name : col_names) {
        auto col_it = tab.get_col(col_name);
        index_cols.push_back(*col_it);
    }
    ix_manager_->destroy_index(tab_name, index_cols);

    auto idx_it = tab.indexes.begin();
    while (idx_it != tab.indexes.end()) {
        std::vector<std::string> idx_col_names;
        for (const auto& col : idx_it->cols) {
            idx_col_names.push_back(col.name);
        }
        if (idx_col_names == col_names) {
            idx_it = tab.indexes.erase(idx_it);
            break;
        } else {
            ++idx_it;
        }
    }

    flush_meta();
}

/**
 * @description: 删除索引（重载版本）
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> col_names;
    for (const auto& col : cols) {
        col_names.push_back(col.name);
    }
    drop_index(tab_name, col_names, context);
}
