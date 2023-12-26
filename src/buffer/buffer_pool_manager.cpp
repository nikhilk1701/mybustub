//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <future>
#include <iterator>
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  std::scoped_lock scp_latch(latch_);
  
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else if(!replacer_->Evict(&fid)) {
    return nullptr;
  }

  if (pages_[fid].IsDirty()) {
    std::promise<bool> callback = disk_scheduler_->CreatePromise();
    std::future<bool> callback_ftr = callback.get_future();

    DiskRequest r = DiskRequest{true, pages_[fid].GetData(), pages_[fid].GetPageId(), std::move(callback)};
    disk_scheduler_->Schedule(std::move(r));

    callback_ftr.get();
  }
  
  page_table_.erase(pages_[fid].GetPageId());
  pages_[fid].ResetMemory();
  page_id_t pid = AllocatePage();
  page_table_[pid] = fid;
  pages_[fid].page_id_ = pid;
  pages_[fid].pin_count_++;
  replacer_->SetEvictable(fid, false);
  replacer_->RecordAccess(fid);

  *page_id = pid;
  return &pages_[fid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock scp_latch(latch_);
  
  if (page_table_.find(page_id) != page_table_.end()) {
    return &pages_[page_table_[page_id]];
  }
  
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else if(!replacer_->Evict(&fid)) {
    return nullptr;
  }

  if (pages_[fid].IsDirty()) {
    std::promise<bool> callback = disk_scheduler_->CreatePromise();
    std::future<bool> callback_ftr = callback.get_future();

    DiskRequest r = DiskRequest{true, pages_[fid].GetData(), pages_[fid].GetPageId(), std::move(callback)};
    disk_scheduler_->Schedule(std::move(r));

    callback_ftr.get();
  }

  page_table_.erase(pages_[fid].GetPageId());
  pages_[fid].ResetMemory();
  page_table_[page_id] = fid;
  pages_[fid].page_id_ = page_id;
  pages_[fid].pin_count_++;
  replacer_->SetEvictable(fid, false);
  replacer_->RecordAccess(fid);

  std::promise<bool> callback = disk_scheduler_->CreatePromise();
  std::future<bool> callback_ftr = callback.get_future();

  DiskRequest r = DiskRequest{false, pages_[fid].GetData(), pages_[fid].GetPageId(), std::move(callback)};
  disk_scheduler_->Schedule(std::move(r));

  callback_ftr.get();

  return &pages_[fid];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock scp_latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return  false;
  }

  frame_id_t fid = page_table_[page_id];
  pages_[fid].is_dirty_ = is_dirty;

  if (pages_[fid].GetPinCount() <= 0) {
    replacer_->SetEvictable(fid, true);
    return false;
  }

  pages_[fid].pin_count_--;
  if (pages_[fid].pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  std::scoped_lock scp_latch(latch_);
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t fid = page_table_[page_id];

  disk_scheduler_->FlushPage(page_id, pages_[fid].GetData());
  pages_[fid].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto it : page_table_) {
    FlushPage(it.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  
  frame_id_t fid = page_table_[page_id];

  if (pages_[fid].pin_count_ > 0) {
    return false;
  }

  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].is_dirty_ = false;
  pages_[fid].pin_count_ = 0;
  pages_[fid].ResetMemory();

  replacer_->Remove(fid);
  free_list_.push_back(fid);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
