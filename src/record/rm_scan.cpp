#include "rm_scan.h"
#include "rm_file_handle.h"

RmScan::RmScan(const RmFileHandle *fh) : file_handle_(fh) {
    rid_.page_no = RM_NO_PAGE;
    rid_.slot_no = -1;
    next();                                        // 定位到第一条记录
}

void RmScan::next() {
    int page = (rid_.page_no == RM_NO_PAGE) ? RM_FIRST_RECORD_PAGE : rid_.page_no;
    int slot = rid_.slot_no;

    while (page < file_handle_->file_hdr_.num_pages) {
        RmPageHandle ph = file_handle_->fetch_page_handle(page);
        int next_slot = Bitmap::next_bit(true, ph.bitmap,
                                         file_handle_->file_hdr_.num_records_per_page, slot);
        file_handle_->buffer_pool_manager_->unpin_page(ph.page->get_page_id(), false);

        if (next_slot < file_handle_->file_hdr_.num_records_per_page) {
            rid_.page_no = page;
            rid_.slot_no = next_slot;
            return;
        }
        ++page;
        slot = -1;
    }
    rid_.page_no = RM_NO_PAGE;
    rid_.slot_no = -1;
}

bool RmScan::is_end() const {
    return rid_.page_no == RM_NO_PAGE;
}

Rid RmScan::rid() const {
    return rid_;
}