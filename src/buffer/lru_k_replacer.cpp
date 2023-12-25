//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <cstdint>
#include <tuple>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode() = default;
LRUKNode::LRUKNode(frame_id_t fid, size_t k) : k_(k), fid_(fid) {};

auto LRUKNode::IsEvictable() -> bool {
    return is_evictable_;
}

auto LRUKNode::GetKRecentAccess(size_t k) -> size_t {
    if (history_.size() < k) {
        return SIZE_MAX;
    }
    return history_.front();
}

auto LRUKNode::GetLeastRecentAccess() -> size_t {
    if (history_.size() > static_cast<size_t>(0)) {
        return history_.front();
    }
    return SIZE_MAX;
}

auto LRUKNode::GetFrameID() -> frame_id_t {
    return fid_;
}

auto LRUKNode::GetSize() -> size_t {
    return history_.size();
}

auto LRUKNode::SetEvictable(bool set_evictable) -> bool {
    if (set_evictable ^ is_evictable_) {
        is_evictable_ = set_evictable;
        return true;
    }
    return false;
}

auto LRUKNode::RecordAccess(size_t timestamp) -> void {
    history_.push_back(timestamp);
    if (history_.size() > k_) {
      history_.pop_front();
    }
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    latch_.lock();
    LRUKNode node = LRUKNode{};
    bool flg = false;
    for (auto & it : node_store_) {
        auto lruk_node = it.second;
        if (it.second.IsEvictable()) {
            if (!flg) {
                node = lruk_node;
                flg = true;
            } else if (node.GetKRecentAccess(k_) != SIZE_MAX && lruk_node.GetKRecentAccess(k_) != SIZE_MAX && node.GetKRecentAccess(k_) > lruk_node.GetKRecentAccess(k_)) {
                node = lruk_node;
            } else if (node.GetKRecentAccess(k_) != lruk_node.GetKRecentAccess(k_) && lruk_node.GetKRecentAccess(k_) == SIZE_MAX) {
                node = lruk_node;
            } else if (node.GetKRecentAccess(k_) == lruk_node.GetKRecentAccess(k_) && node.GetKRecentAccess(k_) == SIZE_MAX) {
                if (node.GetLeastRecentAccess() > lruk_node.GetLeastRecentAccess()) {
                    node = lruk_node;
                }
            }
        }
    }
    if (flg) {
        node_store_.erase(node.GetFrameID());
        curr_size_--;
        *frame_id = node.GetFrameID();
    }
    latch_.unlock();

    return flg;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    latch_.lock();
    if (node_store_.find(frame_id) == node_store_.end()) {
        node_store_[frame_id] = LRUKNode{frame_id, k_};
    }
    current_timestamp_++;
    node_store_[frame_id].RecordAccess(current_timestamp_);
    latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    latch_.lock();
    if (node_store_.find(frame_id) != node_store_.end()) {
        if (node_store_[frame_id].SetEvictable(set_evictable)) {
            if (set_evictable) {
                curr_size_++;
            } else {
                curr_size_--;
            }
        }
    }
    latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    latch_.lock();
    auto it = node_store_.find(frame_id);
    if (it != node_store_.end()) {
        if (it->second.IsEvictable()) {
            curr_size_--;
        }
        node_store_.erase(frame_id);
    }
    latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_;
}

}  // namespace bustub
