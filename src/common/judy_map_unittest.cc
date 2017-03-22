/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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

#ifdef LIZARDFS_HAVE_JUDY
#include "common/judy_map.h"

#include <algorithm>
#include <gtest/gtest.h>



static void fill_map(judy_map<int, int> &m) {
	for (int i = 0; i < 10; ++i) {
		m[i] = i + 100;
	}
}

TEST(JudyMapTest, Constructors) {
	judy_map<int, int> a;

	fill_map(a);

	std::vector<std::pair<int, int>> v = {{1, 1}, {2, 2}, {3, 3}, {4, 4}};

	judy_map<int, int> d(v.begin(), v.end());

	auto first = d.begin();
	auto last = d.end();
	for (; first != last; ++first) {
		ASSERT_EQ((*first).first, (*first).second);
	}

	for (const auto& k : d) {
		ASSERT_EQ(k.first, k.second);
	}
	ASSERT_EQ(d.size(), 4U);

	judy_map<int, int> b(a);
	judy_map<int, int> c(std::move(a));
	a = judy_map<int, int>{{4, 16}, {5, 25}, {6, 36}};

	ASSERT_EQ(b.size(), 10U);
	ASSERT_EQ(c.size(), 10U);
	ASSERT_EQ(b, c);
	ASSERT_TRUE(b != a);
	ASSERT_EQ(a.size(), 3U);
}


TEST(JudyMapTest, Swap) {
	judy_map<int, int> a, b;

	fill_map(a);

	b.insert({1, 1});
	b.insert({11, 11});
	b.insert({111, 111});

	a.swap(b);

	ASSERT_EQ(b.size(), 10U);

	for (int i = 0; i < 10; i++) {
		ASSERT_NE(b.find(i), b.end());
	}

	ASSERT_EQ(a.size(), 3U);
}

TEST(JudyMap, Iterators) {
	std::vector<std::pair<int, int>> v = {{1, 1}, {2, 2}, {3, 3}, {4, 4}};

	judy_map<int, int> d(v.begin(), v.end());

	int i = 0;
	for (const auto &el : d) {
		ASSERT_EQ(el.first, ++i);
	}

	// forward iterators
	i = 1;
	for (judy_map<int, int>::iterator it  = d.begin(); it != d.end(); it++, i++) {
		ASSERT_EQ((*it).second, i);
	}
	i = 1;
	for (judy_map<int, int>::const_iterator it  = d.begin(); it != d.end(); it++, i++) {
		ASSERT_EQ((*it).second, i);
	}
	i = 1;
	for (judy_map<int, int>::const_iterator it  = d.cbegin(); it != d.cend(); it++, i++) {
		ASSERT_EQ((*it).second, i);
	}
}

TEST(JudyMap, InsertErase) {
	judy_map<int, int> m;
	m.insert({1, 0});
	m[2] = 2;
	m[4] = 0;
	m[4] = 0;
	m[3] = 4;
	m[1] = 5;
	m[9] = 0;
	m[4] = 0;
	m[0] = 5;
	m[4] = 0;
	m[5] = 6;
	m[4] = 8;
	m[8] = 7;
	m[4] = 0;
	m[7] = 9;
	m[7] = 0;
	m[3] = 2;
	m[6] = 1;
	m[4] = 0;
	m[0] = 4;

	int i = 0;
	for (auto it = m.begin(); it != m.end(); it++) {
		ASSERT_EQ((*it).first, i++);
	}
	ASSERT_EQ(m.size(), 10U);

	ASSERT_TRUE(m.erase(1));
	m.erase(m.find(0));
	auto it = m.lower_bound(0);
	ASSERT_EQ((*it).first, 2);

	i = 2;
	for (auto it = m.begin(); it != m.end(); it++) {
		ASSERT_EQ((*it).first, i++);
	}
	ASSERT_EQ(m.size(), 8U);

	m[0] = m[3];
	ASSERT_EQ(m.size(), 9U);
	m[3] = m[1];
	ASSERT_EQ(m.size(), 10U);

	for (int i = 0; i < 10; ++i) {
		ASSERT_NO_THROW(m.at(i));
	}
	ASSERT_THROW(m.at(-1), std::out_of_range);
	ASSERT_THROW(m.at(10), std::out_of_range);
}


TEST(JudyMapTest, GeneralBehaviour) {
	judy_map<int, int> map;

	map.insert(judy_map<int, int>::value_type(3, 6));

	auto iterator = const_cast<const judy_map<int, int>&>(map).find(3);

	judy_map<int, int>::value_type value(3, 6);
	EXPECT_EQ(value, *iterator);
	EXPECT_EQ(map.size(), 1U);

	auto pair = static_cast<std::pair<int, int>>(*iterator);
	EXPECT_EQ(value, pair);

	map.erase(3);

	EXPECT_EQ(map.size(), 0U);
	EXPECT_EQ(map.empty(), true);
}

TEST(JudyMapTest, FindNth) {
	std::vector<int> value(100);

	for(int i = 0; i < 100; ++i) {
		value[i] = (rand() % 1000) - 500;
	}

	judy_map<int, int> m;
	for(int i = 0; i < 100; ++i) {
		m[i] = value[i];
	}

	for(int i = 0; i < 100; ++i) {
		EXPECT_EQ((*m.find_nth(i)).second, value[i]);
	}
}

#endif //LIZARDFS_HAVE_JUDY
