#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if ((page_ != nullptr) && (bpm_ != nullptr)) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;

  return *this; 
}

BasicPageGuard::~BasicPageGuard() {
  Drop();
  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  that.guard_ = BasicPageGuard{};
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
  guard_ = std::move(that.guard_);
  that.guard_ = BasicPageGuard{};
  return *this; 
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  that.guard_ = BasicPageGuard{};
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
  guard_ = std::move(that.guard_);
  that.guard_ = BasicPageGuard{};
  return *this; 
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace bustub
