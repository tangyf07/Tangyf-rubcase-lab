#include "rm_file_handle.h"
#include "common/context.h"
#include "storage/buffer_pool_manager.h"
#include "storage/page.h"
#include <memory>
#include <cstring>
#include <algorithm>

#include "transaction/transaction.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/txn_defs.h"

// 【修正后的构造函数】
RmFileHandle::RmFileHandle(DiskManager *disk_manager, BufferPoolManager *bpm, int fd, std::string table_name)
    : disk_manager_(disk_manager), 
      buffer_pool_manager_(bpm), 
      fd_(fd), 
      table_name_(table_name) {

    // 从磁盘读取 header
    disk_manager_->read_page(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    disk_manager_->set_fd2pageno(fd, file_hdr_.num_pages);
}

RmFileHandle::~RmFileHandle() {}

std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid, Context *context) const {
    if (context != nullptr && context->txn_ != nullptr) {
        if (!context->lock_mgr_->lock_IS_on_table(context->txn_, fd_)) {
             throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        if (!context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_)) {
             throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }
    RmPageHandle ph = fetch_page_handle(rid.page_no);
    char *slot = ph.get_slot(rid.slot_no);
    auto rec = std::make_unique<RmRecord>(file_hdr_.record_size);
    memcpy(rec->data, slot, file_hdr_.record_size);
    return rec;
}

Rid RmFileHandle::insert_record(char *buf, Context *context) {
    if (context != nullptr && context->txn_ != nullptr) {
        if (!context->lock_mgr_->lock_IX_on_table(context->txn_, fd_)) {
             throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    RmPageHandle ph = create_page_handle();
    int slot_no = Bitmap::first_bit(false, ph.bitmap, file_hdr_.num_records_per_page);
    Rid rid = {ph.page->get_page_id().page_no, slot_no};

    if (context != nullptr && context->txn_ != nullptr) {
        if (!context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_)) {
             throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    if (context != nullptr && context->txn_ != nullptr) {
        auto *wr = new WriteRecord(WType::INSERT_TUPLE, table_name_, rid);
        context->txn_->append_write_record(wr);
    }

    char *slot = ph.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    Bitmap::set(ph.bitmap, slot_no);
    ph.page_hdr->num_records++;
    
    if (ph.page_hdr->num_records == file_hdr_.num_records_per_page) {
        release_page_handle(ph);
    }
    buffer_pool_manager_->unpin_page(ph.page->get_page_id(), true);
    return rid;
}

void RmFileHandle::insert_record(const Rid &rid, char *buf) {
    RmPageHandle ph = fetch_page_handle(rid.page_no);
    Bitmap::set(ph.bitmap, rid.slot_no);
    ph.page_hdr->num_records++;
    char *slot = ph.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    if (ph.page_hdr->num_records == file_hdr_.num_records_per_page) {
        release_page_handle(ph);
    }
    buffer_pool_manager_->unpin_page(ph.page->get_page_id(), true);
}

void RmFileHandle::delete_record(const Rid &rid, Context *context) {
    if (context != nullptr && context->txn_ != nullptr) {
        if (!context->lock_mgr_->lock_IX_on_table(context->txn_, fd_)) {
             throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        if (!context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_)) {
             throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    RmPageHandle ph = fetch_page_handle(rid.page_no);
    
    if (context != nullptr && context->txn_ != nullptr) {
        char *slot = ph.get_slot(rid.slot_no);
        RmRecord deleted_record(file_hdr_.record_size);
        memcpy(deleted_record.data, slot, file_hdr_.record_size);
        
        auto *wr = new WriteRecord(WType::DELETE_TUPLE, table_name_, rid, deleted_record);
        context->txn_->append_write_record(wr);
    }

    Bitmap::reset(ph.bitmap, rid.slot_no);
    ph.page_hdr->num_records--;
    if (ph.page_hdr->num_records + 1 == file_hdr_.num_records_per_page) {
        ph.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
        file_hdr_.first_free_page_no = ph.page->get_page_id().page_no;
    }
    buffer_pool_manager_->unpin_page(ph.page->get_page_id(), true);
}

void RmFileHandle::update_record(const Rid &rid, char *buf, Context *context) {
    if (context != nullptr && context->txn_ != nullptr) {
        if (!context->lock_mgr_->lock_IX_on_table(context->txn_, fd_)) {
             throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        if (!context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_)) {
             throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    RmPageHandle ph = fetch_page_handle(rid.page_no);
    char *slot = ph.get_slot(rid.slot_no);

    if (context != nullptr && context->txn_ != nullptr) {
        RmRecord old_record(file_hdr_.record_size);
        memcpy(old_record.data, slot, file_hdr_.record_size);
        
        auto *wr = new WriteRecord(WType::UPDATE_TUPLE, table_name_, rid, old_record);
        context->txn_->append_write_record(wr);
    }

    memcpy(slot, buf, file_hdr_.record_size);
    buffer_pool_manager_->unpin_page(ph.page->get_page_id(), true);
}

RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page({fd_, page_no});
    if (!page) throw PageNotExistError("RmFileHandle", page_no);
    return RmPageHandle(&file_hdr_, page);
}

RmPageHandle RmFileHandle::create_new_page_handle() {
    PageId new_page_id{fd_, 0};
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    if (!page) throw InternalError("new_page failed");
    RmPageHdr hdr{};
    hdr.next_free_page_no = file_hdr_.first_free_page_no;
    hdr.num_records = 0;
    memcpy(page->get_data() + page->OFFSET_PAGE_HDR, &hdr, sizeof(hdr));
    char *bitmap = page->get_data() + page->OFFSET_PAGE_HDR + sizeof(RmPageHdr);
    Bitmap::init(bitmap, file_hdr_.bitmap_size);
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = new_page_id.page_no;
    return RmPageHandle(&file_hdr_, page);
}

RmPageHandle RmFileHandle::create_page_handle() {
    int page_no = file_hdr_.first_free_page_no;
    while (page_no != RM_NO_PAGE) {
        RmPageHandle ph = fetch_page_handle(page_no);
        int slot = Bitmap::first_bit(false, ph.bitmap, file_hdr_.num_records_per_page);
        if (slot < file_hdr_.num_records_per_page) {
            return ph;
        }
        page_no = ph.page_hdr->next_free_page_no;
        buffer_pool_manager_->unpin_page(ph.page->get_page_id(), false);
    }
    return create_new_page_handle();
}

void RmFileHandle::release_page_handle(RmPageHandle &ph) {
    int page_no = ph.page->get_page_id().page_no;
    if (file_hdr_.first_free_page_no == page_no) {
        file_hdr_.first_free_page_no = ph.page_hdr->next_free_page_no;
    } else {
        int prev = file_hdr_.first_free_page_no;
        while (prev != RM_NO_PAGE) {
            RmPageHandle prev_ph = fetch_page_handle(prev);
            if (prev_ph.page_hdr->next_free_page_no == page_no) {
                prev_ph.page_hdr->next_free_page_no = ph.page_hdr->next_free_page_no;
                buffer_pool_manager_->unpin_page(prev_ph.page->get_page_id(), true);
                break;
            }
            int next = prev_ph.page_hdr->next_free_page_no;
            buffer_pool_manager_->unpin_page(prev_ph.page->get_page_id(), false);
            prev = next;
        }
    }
    ph.page_hdr->next_free_page_no = RM_NO_PAGE;
}