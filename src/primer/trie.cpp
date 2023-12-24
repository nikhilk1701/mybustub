#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <stack>

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  std::shared_ptr<const TrieNode> current = this->root_;
  auto key_itr = key.begin();
  while (key_itr != key.end()) {
    char key_char = *key_itr;
    if (current == nullptr) {
      break;
    }
    if (current->children_.find(*key_itr) != current->children_.end()) {
      current = current->children_.find(key_char)->second;
    } else {
      current = nullptr;
      break;
    }
    key_itr++;
  }

  if (current == nullptr || !current->is_value_node_) {
    return nullptr;
  }

  const TrieNodeWithValue<T> * val_node = dynamic_cast<const TrieNodeWithValue<T> *>(current.get());

  return val_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if (!root_) {

    std::shared_ptr<const TrieNode> current_node = std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));

    for (auto rit = key.rbegin(); rit != key.rend(); ++rit) {
      auto new_node = std::make_shared<const TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>({{*rit, current_node}}));
      current_node = std::move(new_node);
    }

    return Trie{current_node};
  }

  std::stack<std::shared_ptr<const TrieNode>> node_stack;
  std::shared_ptr<const TrieNode> node = root_;

  auto key_itr = key.begin();
  while (key_itr != key.end()) {
    char key_char = *key_itr;
    node_stack.push(node);
    if (node->children_.find(key_char) == node->children_.end()) { 
      break;
    } else {
      node = node->children_.find(key_char)->second;
    }
    key_itr++;
  }

  if (key_itr == key.end()) {
    // when key is a substring of the prefix

    std::shared_ptr<const TrieNode> current_node = std::make_shared<const TrieNodeWithValue<T>>(node->children_, std::make_shared<T>(std::move(value)));

    for (auto rit = key.rbegin(); rit != key.rend(); ++rit) {
      std::shared_ptr<const TrieNode> old_node = node_stack.top();
      auto new_node = old_node->Clone();
      new_node->children_[*rit] = current_node;
      node_stack.pop();
      current_node = std::move(new_node);
    }

    return Trie{current_node};
  }

  // when prefix is a substring of key
  auto rit = key.rbegin();
  std::shared_ptr<const TrieNode> current_node = std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));

  while(std::distance(key_itr, rit.base()) > 1) {
    auto new_node = std::make_shared<const TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>({{*rit, current_node}}));
    current_node =std::move(new_node);
    ++rit;
  }

  while (!node_stack.empty() && rit != key.rend()) {
    std::shared_ptr<const TrieNode> old_node = node_stack.top();
    auto new_node = old_node->Clone();
    new_node->children_[*rit] = current_node;
    node_stack.pop();
    current_node = std::move(new_node);
  }

  return Trie{current_node};

}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
