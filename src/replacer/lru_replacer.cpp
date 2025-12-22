/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

/**
 * @brief 使用LRU策略删除一个victim frame，这个函数能得到frame_id
 * @param[out] frame_id id of victim frame
 * @return true if a victim frame was found, false otherwise
 */
bool LRUReplacer::victim(frame_id_t *frame_id) {
    std::scoped_lock lock{latch_};
    // 【修正】变量名改为 LRUlist_
    if (LRUlist_.empty()) {
        return false;
    }
    // LRU策略：淘汰链表尾部（最久未使用）
    *frame_id = LRUlist_.back();
    // 【修正】变量名改为 LRUhash_
    LRUhash_.erase(*frame_id);
    LRUlist_.pop_back();
    return true;
}

/**
 * @brief 固定一个frame, 表明它不应该成为victim（即在replacer中移除该frame_id）
 * @param frame_id the id of the frame to pin
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    auto it = LRUhash_.find(frame_id);
    if (it != LRUhash_.end()) {
        LRUlist_.erase(it->second);
        LRUhash_.erase(it);
    }
}

/**
 * @brief 取消固定一个frame, 表明它可以成为victim（即加入replacer）
 * @param frame_id the id of the frame to unpin
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    if (LRUhash_.count(frame_id)) {
        return;
    }
    if (LRUlist_.size() >= max_size_) {
        return;
    }
    // 最近使用：放入链表头部
    LRUlist_.push_front(frame_id);
    LRUhash_[frame_id] = LRUlist_.begin();
}

/**
 * @brief 两个函数返回当前replacer中元素的数量
 */
size_t LRUReplacer::Size() {
    std::scoped_lock lock{latch_};
    return LRUlist_.size();
}