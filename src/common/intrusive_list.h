/*
   Copyright 2016 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <type_traits>

/*! \brief Hook class for intrusive list.
 *
 * Derive a class from this hook in order to store objects of that class in an intrusive list.
 */
class intrusive_list_base_hook {
public:
	intrusive_list_base_hook() : prev_node_(nullptr), next_node_(nullptr) {
	}

	template <class H>
	friend class intrusive_list;
	template <class H>
	friend class intrusive_list_iterator;

protected:
	intrusive_list_base_hook *prev_node_;
	intrusive_list_base_hook *next_node_;
};

/*! \brief Iterator class for double linked list.
 */
template <typename Node>
class intrusive_list_iterator {
public:
	typedef Node *iterator_type;
	typedef std::bidirectional_iterator_tag iterator_category;
	typedef Node value_type;
	typedef Node& reference;
	typedef Node* pointer;
	typedef std::ptrdiff_t difference_type;

	constexpr intrusive_list_iterator() noexcept : current_(nullptr) {
	}

	explicit intrusive_list_iterator(Node *ptr) noexcept : current_(ptr) {
	}

	// Conversion from iterator to const_iterator
	template <typename OtherNode>
	intrusive_list_iterator(
	        const intrusive_list_iterator<typename std::enable_if<
	                std::is_const<OtherNode>::value || !std::is_const<Node>::value,
	                OtherNode>::type> &other) noexcept
	        : current_(const_cast<Node *>(other.current_)) {
	}

	reference operator*() const noexcept {
		return *current_;
	}

	pointer operator->() const noexcept {
		return current_;
	}

	intrusive_list_iterator &operator++() noexcept {
		current_ = static_cast<Node *>(current_->next_node_);
		return *this;
	}

	intrusive_list_iterator operator++(int)noexcept {
		Node *tmp = current_;
		current_ = static_cast<Node *>(current_->next_node_);
		return intrusive_list_iterator(tmp);
	}

	intrusive_list_iterator &operator--() noexcept {
		current_ = static_cast<Node *>(current_->prev_node_);
		return *this;
	}

	intrusive_list_iterator operator--(int)noexcept {
		Node *tmp = current_;
		current_ = static_cast<Node *>(current_->prev_node_);
		return intrusive_list_iterator(tmp);
	}

	Node *current() const {
		return current_;
	}

protected:
	Node *current_;
};

template <typename NodeL, typename NodeR>
inline bool operator==(const intrusive_list_iterator<NodeL> &lhs,
	const intrusive_list_iterator<NodeR> &rhs) noexcept {
	return lhs.current() == rhs.current();
}

template <typename Node>
inline bool operator==(const intrusive_list_iterator<Node> &lhs,
	const intrusive_list_iterator<Node> &rhs) noexcept {
	return lhs.current() == rhs.current();
}

template <typename NodeL, typename NodeR>
inline bool operator!=(const intrusive_list_iterator<NodeL> &lhs,
	const intrusive_list_iterator<NodeR> &rhs) noexcept {
	return lhs.current() != rhs.current();
}

template <typename Node>
inline bool operator!=(const intrusive_list_iterator<Node> &lhs,
	const intrusive_list_iterator<Node> &rhs) noexcept {
	return lhs.current() != rhs.current();
}

template <typename NodeL, typename NodeR>
inline bool operator<(const intrusive_list_iterator<NodeL> &lhs,
	const intrusive_list_iterator<NodeR> &rhs) noexcept {
	return lhs.current() < rhs.current();
}

template <typename Node>
inline bool operator<(const intrusive_list_iterator<Node> &lhs,
	const intrusive_list_iterator<Node> &rhs) noexcept {
	return lhs.current() < rhs.current();
}

template <typename NodeL, typename NodeR>
inline bool operator>(const intrusive_list_iterator<NodeL> &lhs,
	const intrusive_list_iterator<NodeR> &rhs) noexcept {
	return lhs.current() > rhs.current();
}

template <typename Node>
inline bool operator>(const intrusive_list_iterator<Node> &lhs,
	const intrusive_list_iterator<Node> &rhs) noexcept {
	return lhs.current() > rhs.current();
}

template <typename NodeL, typename NodeR>
inline bool operator<=(const intrusive_list_iterator<NodeL> &lhs,
	const intrusive_list_iterator<NodeR> &rhs) noexcept {
	return lhs.current() <= rhs.current();
}

template <typename Node>
inline bool operator<=(const intrusive_list_iterator<Node> &lhs,
	const intrusive_list_iterator<Node> &rhs) noexcept {
	return lhs.current() <= rhs.current();
}

template <typename NodeL, typename NodeR>
inline bool operator>=(const intrusive_list_iterator<NodeL> &lhs,
	const intrusive_list_iterator<NodeR> &rhs) noexcept {
	return lhs.current() >= rhs.current();
}

template <typename Node>
inline bool operator>=(const intrusive_list_iterator<Node> &lhs,
	const intrusive_list_iterator<Node> &rhs) noexcept {
	return lhs.current() >= rhs.current();
}

/*! \brief Intrusive list.
 *
 * Class used for storing elements derived from intrusive_list_base_hook in a double linked list.
 * Compared to std::list intrusive list doesn't take ownership of stored elements,
 * so care must be taken to delete elements after removing them from the list.
 */
template <typename Node>
class intrusive_list {
public:
	typedef Node value_type;
	typedef Node& reference;
	typedef const Node& const_reference;
	typedef Node* pointer;
	typedef const Node* const_pointer;
	typedef intrusive_list_iterator<Node> iterator;
	typedef intrusive_list_iterator<const Node> const_iterator;
	typedef std::size_t size_type;

public:
	intrusive_list() noexcept : front_(), back_(), size_() {
	}

	intrusive_list(const intrusive_list &) = delete;
	intrusive_list(intrusive_list &&other) noexcept : front_(), back_(), size_() {
		swap(other);
	}

	intrusive_list &operator=(const intrusive_list &) = delete;
	intrusive_list &operator=(intrusive_list &&other) noexcept {
		swap(other);
		return *this;
	}

	/*! \brief Destructor.
	 *
	 * Note that destructor doesn't delete/destroy elements stored in list.
	 */
	~intrusive_list() {
	}

	reference front() {
		assert(!empty());
		return *front_;
	}

	reference back() {
		assert(!empty());
		return *back_;
	}

	const_reference front() const {
		assert(!empty());
		return *front_;
	}

	const_reference back() const {
		assert(!empty());
		return *back_;
	}

	void pop_front() {
		assert(front_ && back_);
		Node *tmp = front_;
		front_ = static_cast<Node *>(front_->next_node_);
		if (!front_) {
			back_ = nullptr;
		} else {
			front_->prev_node_ = nullptr;
		}
		tmp->prev_node_ = nullptr;
		tmp->next_node_ = nullptr;
		--size_;
	}

	void pop_back() {
		assert(front_ && back_);
		Node *tmp = back_;
		back_ = static_cast<Node*>(back_->prev_node_);
		if (!back_) {
			assert(front_ == tmp);
			front_ = nullptr;
		} else {
			back_->next_node_ = nullptr;
		}
		tmp->prev_node_ = nullptr;
		tmp->next_node_ = nullptr;
		--size_;
	}

	/*! \brief Remove and destroy first element from list.
	 *
	 * \param disposer Function object used for destroying elements removed from list.
	 */
	template<typename Disposer>
	void pop_front_and_dispose(Disposer disposer) {
		Node *tmp = front_;
		pop_front();
		disposer(tmp);
	}

	/*! \brief Remove and destroy last element from list.
	 *
	 * \param disposer Function object used for destroying elements removed from list.
	 */
	template<typename Disposer>
	void pop_back_and_dispose(Disposer disposer) {
		Node *tmp = back_;
		pop_back();
		disposer(tmp);
	}

	void push_back(value_type &h) {
		h.next_node_ = nullptr;
		h.prev_node_ = back_;
		if (back_) {
			back_->next_node_ = std::addressof(h);
			back_ = std::addressof(h);
		} else {
			front_ = back_ = std::addressof(h);
		}
		++size_;
	}

	void push_front(value_type &h) {
		h.next_node_ = front_;
		h.prev_node_ = nullptr;
		if (front_) {
			front_->prev_node_ = std::addressof(h);
			front_ = std::addressof(h);
		} else {
			front_ = back_ = std::addressof(h);
		}
		++size_;
	}

	void clear() {
		while (front_) {
			Node *tmp = front_;
			front_ = static_cast<Node *>(front_->next_node_);
			tmp->prev_node_ = nullptr;
			tmp->next_node_ = nullptr;
		}
		back_ = nullptr;
		size_ = 0;
	}

	/*! \brief Remove and destroy all elements from list.
	 *
	 * \param disposer Function object used for destroying elements removed from list.
	 */
	template<typename Disposer>
	void clear_and_dispose(Disposer disposer) {
		while (front_) {
			Node *tmp = front_;
			front_ = static_cast<Node *>(front_->next_node_);
			tmp->prev_node_ = nullptr;
			tmp->next_node_ = nullptr;
			disposer(tmp);
		}
		back_ = nullptr;
		size_ = 0;
	}

	iterator erase(iterator position) {
		Node *node = position.current();

		if (!node) {
			return end();
		}

		Node *prev = static_cast<Node*>(node->prev_node_);
		Node *next = static_cast<Node*>(node->next_node_);

		if (prev) {
			prev->next_node_ = next;
		} else {
			front_ = next;
		}
		if (next) {
			next->prev_node_ = prev;
		} else {
			back_ = prev;
		}
		--size_;

		node->prev_node_ = nullptr;
		node->next_node_ = nullptr;

		return iterator(next);
	}

	/*! \brief Remove and destroy single element.
	 *
	 * \param position Iterator pointing to a single element to be removed from the list.
	 * \param disposer Function object used for destroying elements removed from list.
	 */
	template<typename Disposer>
	iterator erase_and_dispose(iterator position, Disposer disposer) {
		Node *node = position.current();
		auto it = erase(position);
		if (node) {
			disposer(node);
		}
		return it;
	}

	iterator insert(iterator position, value_type &node) {
		Node *next = position.current();
		Node *prev = next ? static_cast<Node*>(next->prev_node_) : nullptr;

		if (prev) {
			prev->next_node_ = std::addressof(node);
		} else {
			front_ = std::addressof(node);
		}
		if (next) {
			next->prev_node_ = std::addressof(node);
		} else {
			back_ = std::addressof(node);
		}
		++size_;

		node.prev_node_ = prev;
		node.next_node_ = next;

		return iterator(std::addressof(node));
	}

	bool empty() const {
		return front_ == nullptr;
	}

	size_type size() const {
		return size_;
	}

	void swap(intrusive_list &other) noexcept {
		std::swap(front_, other.front_);
		std::swap(back_, other.back_);
		std::swap(size_, other.size_);
	}

	iterator begin() {
		return iterator(front_);
	}

	iterator end() {
		return iterator(nullptr);
	}

	const_iterator begin() const {
		return const_iterator(front_);
	}

	const_iterator end() const {
		return const_iterator(nullptr);
	}

	const_iterator cbegin() const {
		return const_iterator(front_);
	}

	const_iterator cend() const {
		return const_iterator(nullptr);
	}

	void splice(iterator position, intrusive_list &x) {
		if (x.empty()) {
			return;
		}

		Node *node_next = position.current();
		Node *node_prev = node_next ? static_cast<Node*>(node_next->prev_node_) : back_;

		if (node_prev) {
			node_prev->next_node_ = x.front_;
		} else {
			front_ = x.front_;
		}

		x.front_->prev_node_ = node_prev;
		x.back_->next_node_ = node_next;

		if (node_next) {
			node_next->prev_node_ = x.back_;
		} else {
			back_ = x.back_;
		}

		size_ += x.size_;

		x.front_ = nullptr;
		x.back_ = nullptr;
		x.size_ = 0;
	}

private:
	Node *front_;
	Node *back_;
	size_type size_;
};
