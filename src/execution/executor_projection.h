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

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    void beginTuple() override {
    prev_->beginTuple();  // 让子节点定位到第一条
}

    void nextTuple() override {
    prev_->nextTuple();  // 让子节点向后移动一条
}

    std::unique_ptr<RmRecord> Next() override {
    if (is_end()) return nullptr;

    // 1. 从子节点拿整条原始记录
    auto raw_rec = prev_->Next();
    if (!raw_rec) return nullptr;

    // 2. 创建投影后的记录缓冲区
    auto proj_rec = std::make_unique<RmRecord>(len_);
    char *dst = proj_rec->data;

    // 3. 按 sel_idxs_ 把需要的列拷贝到新区
    for (size_t i = 0; i < sel_idxs_.size(); ++i) {
        const ColMeta &col = cols_[i];
        const char *src = raw_rec->data + prev_->cols()[sel_idxs_[i]].offset;
        std::memcpy(dst + col.offset, src, col.len);
    }
    return proj_rec;
}

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override {
    return prev_->is_end();  // 子节点没数据了，投影也就结束了
}
    
    std::string getType() override { return "ProjectionExecutor"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }
};