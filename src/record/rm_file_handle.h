/* src/record/rm_file_handle.h */
#pragma once

#include <assert.h>
#include <memory>
#include <string> // 必需

#include "bitmap.h"
#include "common/context.h"
#include "rm_defs.h"

class RmManager;

/* 对表数据文件中的页面进行封装 */
struct RmPageHandle {
    const RmFileHdr *file_hdr;
    Page *page;
    RmPageHdr *page_hdr;
    char *bitmap;
    char *slots;

    RmPageHandle(const RmFileHdr *fhdr_, Page *page_) : file_hdr(fhdr_), page(page_) {
        page_hdr = reinterpret_cast<RmPageHdr *>(page->get_data() + page->OFFSET_PAGE_HDR);
        bitmap = page->get_data() + sizeof(RmPageHdr) + page->OFFSET_PAGE_HDR;
        slots = bitmap + file_hdr->bitmap_size;
    }

    char* get_slot(int slot_no) const {
        return slots + slot_no * file_hdr->record_size;
    }
};

/* 文件句柄类 */
class RmFileHandle {
    friend class RmScan;
    friend class RmManager;

   private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    int fd_;
    RmFileHdr file_hdr_;
    
    // 【关键】新增成员变量
    std::string table_name_;

   public:
    // 【关键】构造函数声明（注意这里没有分号后面的冒号初始化列表）
    RmFileHandle(DiskManager *disk_manager, BufferPoolManager *bpm, int fd, std::string table_name);

    // 析构函数
    ~RmFileHandle();

    RmFileHdr get_file_hdr() { return file_hdr_; }
    int GetFd() { return fd_; }

    bool is_record(const Rid &rid) const {
        RmPageHandle page_handle = fetch_page_handle(rid.page_no);
        return Bitmap::is_set(page_handle.bitmap, rid.slot_no);
    }

    std::unique_ptr<RmRecord> get_record(const Rid &rid, Context *context) const;
    Rid insert_record(char *buf, Context *context);
    void insert_record(const Rid &rid, char *buf);
    void delete_record(const Rid &rid, Context *context);
    void update_record(const Rid &rid, char *buf, Context *context);

    RmPageHandle create_new_page_handle();
    RmPageHandle fetch_page_handle(int page_no) const;

   private:
    RmPageHandle create_page_handle();
    void release_page_handle(RmPageHandle &page_handle);
};