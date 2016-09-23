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

#include "common/platform.h"
#include "common/intrusive_list.h"

#include <algorithm>
#include <numeric>
#include <gtest/gtest.h>

class Node : public intrusive_list_base_hook {
public:
	Node(int a = 0) : value_(a) {}

	operator int() const {
		return value_;
	}

protected:
	int value_;
};

inline bool operator==(const intrusive_list<Node> &a, const std::vector<int> &b) {
	return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

inline bool operator!=(const intrusive_list<Node> &a, const std::vector<int> &b) {
	return !(a == b);
}

TEST(IntrusiveList, PushBack) {
	std::vector<int> vec(100);
	intrusive_list<Node> list;

	std::iota(vec.begin(), vec.end(), 1);

	for(const auto &value : vec) {
		auto node = new Node(value);
		list.push_back(*node);
	}

	EXPECT_EQ(list, vec);

	list.clear_and_dispose([](Node *ptr) { delete ptr; });
}

TEST(IntrusiveList, Erase) {
	std::vector<int> vec(100);
	intrusive_list<Node> list;

	std::iota(vec.begin(), vec.end(), 1);
	for(const auto &value : vec) {
		auto node = new Node(value);
		list.push_back(*node);
	}

	list.erase(std::next(list.begin(), 10));
	vec.erase(std::next(vec.begin(), 10));

	EXPECT_EQ(list, vec);

	list.clear_and_dispose([](Node *ptr) { delete ptr; });
}

TEST(IntrusiveList, Insert) {
	std::vector<int> vec(100);
	intrusive_list<Node> list;

	std::iota(vec.begin(), vec.end(), 1);
	for(const auto &value : vec) {
		auto node = new Node(value);
		list.push_back(*node);
	}

	auto node = new Node(446);
	vec.insert(std::next(vec.begin(), 33), 446);
	list.insert(std::next(list.begin(), 33), *node);

	EXPECT_EQ(list, vec);

	list.clear_and_dispose([](Node *ptr) { delete ptr; });
}

TEST(IntrusiveList, Splice) {
	std::vector<int> vec1(100), vec2(33);
	intrusive_list<Node> list1, list2;

	std::iota(vec1.begin(), vec1.end(), 1);
	std::iota(vec2.begin(), vec2.end(), 332);

	for(const auto &value : vec1) {
		auto node = new Node(value);
		list1.push_back(*node);
	}
	for(const auto &value : vec2) {
		auto node = new Node(value);
		list2.push_back(*node);
	}

	vec1.insert(std::next(vec1.begin(), 77), vec2.begin(), vec2.end());
	list1.splice(std::next(list1.begin(), 77), list2);

	EXPECT_EQ(list1, vec1);

	list1.clear_and_dispose([](Node *ptr) { delete ptr; });
}
