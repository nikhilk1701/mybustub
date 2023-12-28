//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <cstddef>
#include <optional>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  std::optional<DiskRequest> req_opt = std::make_optional(std::move(r));
  request_queue_.Put(std::move(req_opt));
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    std::optional<DiskRequest> r = request_queue_.Get();
    if (!r.has_value()) {
      break;
    }

    if (r->is_write_) {
      disk_manager_->WritePage(r->page_id_, r->data_);
    } else {
      disk_manager_->ReadPage(r->page_id_, r->data_);
    }

    r->callback_.set_value(true);
  }
}

void DiskScheduler::FlushPage(page_id_t page_id, const char *page_data) {
  disk_manager_->WritePage(page_id, page_data);
}

}  // namespace bustub
