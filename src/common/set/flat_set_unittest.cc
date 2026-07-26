/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#include "flat_set.h"
#include "common/platform.h"

#include <cassert>
#include <gtest/gtest.h>
#include <iterator>

void simple_insert(flat_set<int> &cs) {
	for (int i = 0; i < 10 ; i++) {
		cs.insert(i);
	}
}


TEST(FlatSet, RedundantInsert) {
	flat_set<int> cs;
	cs.insert(1);
	cs.insert(2);
	cs.insert(1);
	cs.insert(4);
	cs.insert(1);
	cs.insert(4);
	cs.insert(1);
	cs.insert(3);
	cs.insert(4);
	cs.insert(5);
	cs.insert(4);
	cs.insert(4);
	cs.insert(1);
	cs.insert(7);
	cs.insert(1);
	cs.insert(4);
	cs.insert(4);
	cs.insert(6);
	cs.insert(4);
	cs.insert(4);
	cs.insert(0);

	int i = 0;
	for (auto it = cs.begin(); it != cs.end(); it++, i++) {
		ASSERT_EQ(*it, i);
	}

	// const &
	std::initializer_list<std::string> elems2 = {"0", "1", "2", "3", "4"};
	flat_set<std::string> cs2(elems2);
	for (char const* s : {"2", "4"}) {
		EXPECT_EQ(cs2.size(), elems2.size());
		cs2.insert(s);
		EXPECT_EQ(cs2.size(), elems2.size());
	}
}

TEST(FlatSet, HintInsert) {
	auto elems1 = {0, 1, 2, 3, 4};
	flat_set<int> cs1(elems1, true);
	int value1 = 2;
	// insert(hint, &&)
	for(unsigned index = 0; index <= cs1.size(); ++index) {
		EXPECT_EQ(cs1.size(), elems1.size());
		auto tmp_value = value1;
		auto result = cs1.insert(cs1.begin() + index, std::move(tmp_value));
		EXPECT_EQ(result, cs1.find(value1));
		EXPECT_EQ(cs1.size(), elems1.size());
	}

	// insert(hint, const &)
	std::initializer_list<std::string> elems2 = {"0", "1", "2", "3", "4"};
	flat_set<std::string> cs2(elems2, true);
	const std::string value2("2");
	for(unsigned index = 0; index <= cs2.size(); ++index) {
		EXPECT_EQ(cs2.size(), elems2.size());
		auto result = cs2.insert(cs2.begin() + index, value2);
		EXPECT_EQ(result, cs2.find(value2));
		EXPECT_EQ(cs2.size(), elems2.size());
	}
}

TEST(FlatSet, RangeInsert) {
	flat_set<int> a{7};

	a.insert(10);
	a.insert(0);

	std::vector<int> c{9, 6, 8};
	a.insert(c.begin(), c.end());
	for (auto i : c) {
		ASSERT_TRUE(a.count(i));
	}

	a.insert({1, 4, 3, 4, 2, 5});
	for (int i = 0; i < 10; i++) {
		ASSERT_TRUE(a.count(i));
	}
}


TEST(FlatSet, EqualRange) {
	flat_set<int> cs;
	simple_insert(cs);

	auto p = cs.equal_range(1);
	ASSERT_EQ(std::distance(p.first, p.second), 1);

	p = cs.equal_range(10);
	ASSERT_EQ(std::distance(p.first, p.second), 0);

	cs.clear();
	p = cs.equal_range(1);
	ASSERT_EQ(std::distance(p.first, p.second),  0);
}


TEST(FlatSet, SimpleIterators) {
	flat_set<int> cs;
	simple_insert(cs);

	for (int i = 0; i < 10; i++) {
		ASSERT_EQ(*cs.find(i), i);
	}

	for (int i = 10; i < 20; i++) {
		ASSERT_EQ(cs.find(i), cs.end());
	}
}


TEST(FlatSet, Swap) {
	flat_set<int> a, b;
	simple_insert(a);

	b.insert(1);
	b.insert(11);
	b.insert(111);

	a.swap(b);

	ASSERT_EQ(b.size(), 10U);

	for (int i = 0; i < 10; i++) {
		ASSERT_TRUE(b.count(i));
	}

	ASSERT_EQ(a.size(), 3U);

	ASSERT_TRUE(a.count(1));
	ASSERT_TRUE(a.count(11));
	ASSERT_TRUE(a.count(111));
}

TEST(FlatSet, Erase) {
	flat_set<int> a;
	simple_insert(a);

	for (int i = 0; i < 4; ++i) {
		ASSERT_TRUE(a.erase(i));
	}
	auto it = a.erase(a.find(4));
	ASSERT_EQ(*it, 5);

	ASSERT_FALSE(a.erase(0));

	ASSERT_EQ(a.size(), 5U);
	for (int i = 5; i < 10; ++i) {
		ASSERT_TRUE(a.find(i) != a.end());
	}
}

TEST(FlatSet, Constructors) {
	flat_set<int> a;
	simple_insert(a);

	flat_set<int> b(a);
	flat_set<int> c(std::move(a));

	ASSERT_EQ(b.size(), 10U);
	ASSERT_EQ(c.size(), 10U);

	std::vector<int> v1{1, 2, 3, 4, 5};
	flat_set<int> d(v1.begin(), v1.end(), true);
	ASSERT_EQ(d.size(), 5U);
	ASSERT_EQ(d.data(), v1);

	std::vector<int> v2{4, 2, 5, 1, 3, 4};
	flat_set<int> e(v2.begin(), v2.end());
	ASSERT_EQ(e.size(), 5U);
	ASSERT_EQ(e.data(), v1);

	flat_set<int> g{-1, 0, 1, 2, 3, 4, 5, 6, 7, 1, 1, 0};


	ASSERT_EQ(g.size(), 9U);

	for (int i = -1; i < 8; i++) {
		ASSERT_TRUE(g.find(i) != g.end());
	}

	flat_set<int> h({1, 2, 3, 4, 5}, true);
	ASSERT_EQ(h.size(), 5U);
	ASSERT_EQ(h, e);

}

TEST(FlatSet, Assign) {
	flat_set<int> a;
	simple_insert(a);
	flat_set<int> b = a;
	ASSERT_EQ(a, b);

	a = {1,2,3,4};
	ASSERT_EQ(a, flat_set<int>({1, 2, 3, 4}));

	a = b;
	ASSERT_EQ(a, b);
}

