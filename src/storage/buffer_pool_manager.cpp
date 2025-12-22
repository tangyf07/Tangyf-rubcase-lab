/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // 1. 检查空闲链表
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    // 2. 检查 Replacer
    if (replacer_->victim(frame_id)) {
        return true;
    }
    return false;
}

Page* BufferPoolManager::fetch_page(PageId page_id) {
    std::scoped_lock lock{latch_};
    // 1. 在页表中查找
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t fid = it->second;
        replacer_->pin(fid);
        pages_[fid].pin_count_++;
        return &pages_[fid];
    }

    // 2. 获取替换页
    frame_id_t fid;
    if (!find_victim_page(&fid)) {
        return nullptr;
    }

    Page* page = &pages_[fid];
    
    // 3. 写回脏页
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }

    // 4. 从页表中移除旧页
    page_table_.erase(page->id_);

    // 5. 读入新页
    page_table_[page_id] = fid;
    page->id_ = page_id;
    page->pin_count_ = 1;
    disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    replacer_->pin(fid);

    return page;
}

bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    std::scoped_lock lock{latch_};
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;

    frame_id_t fid = it->second;
    Page* page = &pages_[fid];

    if (page->pin_count_ <= 0) return false;

    if (is_dirty) page->is_dirty_ = true;

    page->pin_count_--;
    if (page->pin_count_ == 0) {
        replacer_->unpin(fid);
    }
    return true;
}

bool BufferPoolManager::flush_page(PageId page_id) {
    std::scoped_lock lock{latch_};
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;
    
    frame_id_t fid = it->second;
    Page* page = &pages_[fid];
    disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
    page->is_dirty_ = false;
    return true;
}

Page* BufferPoolManager::new_page(PageId* page_id) {
    std::scoped_lock lock{latch_};
    frame_id_t fid;
    // 1. 获取 victim frame
    if (!find_victim_page(&fid)) {
        return nullptr;
    }

    Page* page = &pages_[fid];

    // 2. 写回脏页
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }

    // 3. 更新页表
    page_table_.erase(page->id_);

    // 4. 分配新页号
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);
    
    // 5. 初始化新页
    page->id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false; // 新页初始为非脏，虽然内存可能是脏的，但逻辑上是新空白页
    // 注意：Rucbase 测试通常要求 new_page 返回的页内容清零，或者直接覆盖使用
    memset(page->data_, 0, PAGE_SIZE); 

    page_table_[*page_id] = fid;
    replacer_->pin(fid);

    return page;
}

bool BufferPoolManager::delete_page(PageId page_id) {
    std::scoped_lock lock{latch_};
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return true;

    frame_id_t fid = it->second;
    Page* page = &pages_[fid];

    if (page->pin_count_ > 0) return false;

    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }

    page_table_.erase(page_id);
    page->id_.page_no = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
    
    // 归还到空闲链表
    free_list_.push_back(fid);
    return true;
}

void BufferPoolManager::flush_all_pages(int fd) {
    std::scoped_lock lock{latch_};
    for (const auto& entry : page_table_) {
        if (entry.first.fd == fd) {
            Page* page = &pages_[entry.second];
            if (page->is_dirty_) {
                disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
                page->is_dirty_ = false;
            }
        }
    }
}