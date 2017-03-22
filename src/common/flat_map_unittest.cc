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

#include "flat_map.h"
#include "common/platform.h"

#include <cassert>
#include <gtest/gtest.h>
#include <iterator>
#include <string>

void fill_map(flat_map<int, int> &m) {
	for (int i = 0; i < 10; ++i) {
		m[i] = i + 100;
	}
}

TEST(FlatMap, Constructors) {
	flat_map<int, int> a;
	fill_map(a);

	std::vector<std::pair<int, int>> v = {{1, 1}, {2, 2}, {3, 3}, {4, 4}};

	flat_map<int, int> d(v.begin(), v.end(), true);
	for (const auto& k : d) {
		ASSERT_EQ(k.first, k.second);
	}
	ASSERT_EQ(d.size(), 4U);

	flat_map<int, int> b(a);
	flat_map<int, int> c(std::move(a));
	a = flat_map<int, int>{{4, 16}, {5, 25}, {6, 36}};

	ASSERT_EQ(b.size(), 10U);
	ASSERT_EQ(c.size(), 10U);
	ASSERT_EQ(b.data(), c.data());
	ASSERT_EQ(b, c);
	ASSERT_TRUE(b != a);
	ASSERT_EQ(a.size(), 3U);
}

TEST(FlatMap, Swap) {
	flat_map<int, int> a, b;
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

TEST(FlatMap, Iterators) {
	std::vector<std::pair<int, int>> v = {{1, 1}, {2, 2}, {3, 3}, {4, 4}};

	typedef flat_map<int, int> test_map;
	test_map d(v.begin(), v.end(), true);

	int i = 0;
	for (const auto &el : d) {
		ASSERT_EQ(el.first, ++i);
	}

	// forward iterators
	i = 1;
	for (test_map::iterator it  = d.begin(); it != d.end(); it++, i++) {
		ASSERT_EQ(it->second, i);
	}
	i = 1;
	for (test_map::const_iterator it  = d.begin(); it != d.end(); it++, i++) {
		ASSERT_EQ(it->second, i);
	}
	i = 1;
	for (test_map::const_iterator it  = d.cbegin(); it != d.cend(); it++, i++) {
		ASSERT_EQ(it->second, i);
	}

	// reverse iterators
	i = d.size();
	for (test_map::reverse_iterator it  = d.rbegin(); it != d.rend(); it++, i--) {
		ASSERT_EQ(it->second, i);
	}
	i = d.size();
	for (test_map::const_reverse_iterator it  = d.rbegin(); it != d.rend(); it++, i--) {
		ASSERT_EQ(it->second, i);
	}
	i = d.size();
	for (test_map::const_reverse_iterator it  = d.crbegin(); it != d.crend(); it++, i--) {
		ASSERT_EQ(it->second, i);
	}
}

TEST(FlatMap, InsertErase) {
	flat_map<int, int> m;
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
		ASSERT_EQ(it->first, i++);
	}
	ASSERT_EQ(m.size(), 10U);

	ASSERT_TRUE(m.erase(1));
	auto it = m.erase(m.find(0));
	ASSERT_EQ(it->first, 2);

	i = 2;
	for (auto it = m.begin(); it != m.end(); it++) {
		ASSERT_EQ(it->first, i++);
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

	flat_map<char, int> ctoi;

	ctoi['e'] = 1;
	ctoi['b'] = 4;
	ctoi['c'] = 5;
	ctoi['d'] = 9;
	ctoi['a'] = 91;
	ctoi['a'] = 1;
	ctoi['a'] = 6;
	ctoi['a'] = 1;
	ctoi['f'] = 5;
	ctoi['a'] = 1;
	ctoi['g'] = 3;
	ctoi['g'] = 1;
	ctoi['e'] = 8;
	ctoi['a'] = 1;
	ctoi['a'] = 3;
	ctoi['d'] = 1;
	ctoi['a'] = 1;
	ctoi['a'] = 12;
	ctoi['r'] = 1;
	ctoi['a'] = 54;
	ctoi['x'] = 1;
	ctoi['a'] = 222;

	EXPECT_EQ(ctoi['a'], 222);
	EXPECT_EQ(ctoi['b'], 4);
	EXPECT_EQ(ctoi['c'], 5);
	EXPECT_EQ(ctoi['d'], 1);
	EXPECT_EQ(ctoi['e'], 8);
	EXPECT_EQ(ctoi['f'], 5);
	EXPECT_EQ(ctoi['g'], 1);
	EXPECT_EQ(ctoi['r'], 1);
	EXPECT_EQ(ctoi['x'], 1);
	ASSERT_EQ(ctoi.size(), 9U);
}

TEST(FlatMap, At) {
	flat_map<std::string, int> stoi;

	stoi["heimerdinger"] = 8;
	stoi["miss fortune"] = 2;
	stoi["diana"] = 15;
	stoi["evelyn"] = 3;
	stoi["lissandra"] = 5;
	stoi["fiora"] = 2;
	stoi["gnar"] = 8;
	stoi["braum"] = 7;
	stoi["cassiopeia"] = 9;
	stoi["annie"] = 4;
	stoi["irelia"] = 9;
	stoi["jayce"] = 2;

	EXPECT_EQ(stoi.at("annie"), 4);
	EXPECT_EQ(stoi.at("braum"), 7);
	EXPECT_EQ(stoi.at("cassiopeia"), 9);
	EXPECT_EQ(stoi.at("diana"), 15);
	EXPECT_EQ(stoi.at("evelyn"), 3);
	EXPECT_EQ(stoi.at("fiora"), 2);
	EXPECT_EQ(stoi.at("gnar"), 8);
	EXPECT_EQ(stoi.at("heimerdinger"), 8);
	EXPECT_EQ(stoi.at("irelia"), 9);
	EXPECT_EQ(stoi.at("jayce"), 2);
	EXPECT_EQ(stoi.at("lissandra"), 5);
	EXPECT_EQ(stoi.at("miss fortune"), 2);

	ASSERT_THROW(stoi.at("Nunu"), std::out_of_range);
	ASSERT_THROW(stoi.at("Orianna"), std::out_of_range);
	ASSERT_THROW(stoi.at("Annie"), std::out_of_range);
}

TEST(FlatMap, Erase) {
	flat_map<std::string, int> stoi;

	stoi["heimerdinger"] = 8;
	stoi["miss fortune"] = 2;
	stoi["diana"] = 15;
	stoi["evelyn"] = 3;
	stoi["lissandra"] = 5;
	stoi["fiora"] = 2;
	stoi["gnar"] = 8;
	stoi["braum"] = 7;
	stoi["cassiopeia"] = 9;
	stoi["annie"] = 4;
	stoi["irelia"] = 9;
	stoi["jayce"] = 2;

	EXPECT_EQ(stoi.at("annie"), 4);
	EXPECT_EQ(stoi.at("braum"), 7);
	EXPECT_EQ(stoi.at("cassiopeia"), 9);
	EXPECT_EQ(stoi.at("diana"), 15);
	EXPECT_EQ(stoi.at("evelyn"), 3);
	EXPECT_EQ(stoi.at("fiora"), 2);
	EXPECT_EQ(stoi.at("gnar"), 8);
	EXPECT_EQ(stoi.at("heimerdinger"), 8);
	EXPECT_EQ(stoi.at("irelia"), 9);
	EXPECT_EQ(stoi.at("jayce"), 2);
	EXPECT_EQ(stoi.at("lissandra"), 5);
	EXPECT_EQ(stoi.at("miss fortune"), 2);

	ASSERT_THROW(stoi.at("Nunu"), std::out_of_range);
	ASSERT_THROW(stoi.at("Orianna"), std::out_of_range);
	ASSERT_THROW(stoi.at("Annie"), std::out_of_range);

	stoi.erase("heimerdinger");
	stoi.erase("braum");
	stoi.erase("heimerdinger");

	EXPECT_EQ(stoi.at("annie"), 4);
	EXPECT_EQ(stoi.at("cassiopeia"), 9);
	EXPECT_EQ(stoi.at("diana"), 15);
	EXPECT_EQ(stoi.at("evelyn"), 3);
	EXPECT_EQ(stoi.at("fiora"), 2);
	EXPECT_EQ(stoi.at("gnar"), 8);
	EXPECT_EQ(stoi.at("irelia"), 9);
	EXPECT_EQ(stoi.at("jayce"), 2);
	EXPECT_EQ(stoi.at("lissandra"), 5);
	EXPECT_EQ(stoi.at("miss fortune"), 2);

	ASSERT_THROW(stoi.at("Nunu"), std::out_of_range);
	ASSERT_THROW(stoi.at("Orianna"), std::out_of_range);
	ASSERT_THROW(stoi.at("Annie"), std::out_of_range);
	ASSERT_THROW(stoi.at("heimerdinger"), std::out_of_range);
	ASSERT_THROW(stoi.at("braum"), std::out_of_range);

	stoi.erase(stoi.begin());

	EXPECT_EQ(stoi.at("cassiopeia"), 9);
	EXPECT_EQ(stoi.at("diana"), 15);
	EXPECT_EQ(stoi.at("evelyn"), 3);
	EXPECT_EQ(stoi.at("fiora"), 2);
	EXPECT_EQ(stoi.at("gnar"), 8);
	EXPECT_EQ(stoi.at("irelia"), 9);
	EXPECT_EQ(stoi.at("jayce"), 2);
	EXPECT_EQ(stoi.at("lissandra"), 5);
	EXPECT_EQ(stoi.at("miss fortune"), 2);

	ASSERT_THROW(stoi.at("Nunu"), std::out_of_range);
	ASSERT_THROW(stoi.at("Orianna"), std::out_of_range);
	ASSERT_THROW(stoi.at("Annie"), std::out_of_range);
	ASSERT_THROW(stoi.at("heimerdinger"), std::out_of_range);
	ASSERT_THROW(stoi.at("braum"), std::out_of_range);
	ASSERT_THROW(stoi.at("annie"), std::out_of_range);

	auto first = stoi.find("evelyn");
	auto last = stoi.find("jayce");

	stoi.erase(first, last);

	EXPECT_EQ(stoi.at("cassiopeia"), 9);
	EXPECT_EQ(stoi.at("diana"), 15);
	EXPECT_EQ(stoi.at("jayce"), 2);
	EXPECT_EQ(stoi.at("lissandra"), 5);
	EXPECT_EQ(stoi.at("miss fortune"), 2);

	ASSERT_THROW(stoi.at("Nunu"), std::out_of_range);
	ASSERT_THROW(stoi.at("Orianna"), std::out_of_range);
	ASSERT_THROW(stoi.at("Annie"), std::out_of_range);
	ASSERT_THROW(stoi.at("heimerdinger"), std::out_of_range);
	ASSERT_THROW(stoi.at("braum"), std::out_of_range);
	ASSERT_THROW(stoi.at("annie"), std::out_of_range);
	ASSERT_THROW(stoi.at("evelyn"), std::out_of_range);
	ASSERT_THROW(stoi.at("fiora"), std::out_of_range);
	ASSERT_THROW(stoi.at("gnar"), std::out_of_range);
	ASSERT_THROW(stoi.at("irelia"), std::out_of_range);
}

TEST(FlatMap, Count) {
	flat_map<std::string, std::string> stos;

	stos["ilidan"] = "stormrage";
	stos["malfurion"] = "stormrage";
	stos["sylvanas"] = "windrunner";
	stos["grommash"] = "hellscream";

	EXPECT_EQ(stos.count("ilidan"), 1U);
	EXPECT_EQ(stos.count("malfurion"), 1U);
	EXPECT_EQ(stos.count("arthas"), 0U);

}

class CCmp {
public:
	bool operator()(const std::string &a, const std::string &b) const {
		return b < a;
	}
};

TEST(FlatMap, CustomCompare) {
	flat_map<std::string,
	std::string,
	std::vector<std::pair<std::string,std::string>>,
	CCmp>
		stos{CCmp()};

	stos["ilidan"] = "stormrage";
	stos["malfurion"] = "stormrage";
	stos["sylvanas"] = "windrunner";
	stos["grommash"] = "hellscream";
	EXPECT_EQ(stos.begin()->first, "sylvanas");
}

TEST(FlatMap, Find) {
	flat_map<std::string,
	std::string,
	std::vector<std::pair<std::string,std::string>>,
	CCmp>
		stos{CCmp()};

	stos["ilidan"] = "stormrage";
	stos["malfurion"] = "stormrage";
	stos["sylvanas"] = "windrunner";
	stos["grommash"] = "hellscream";

	EXPECT_EQ(stos.find("ilidan")->second, "stormrage");
	EXPECT_EQ(stos.find("sylvanas")->second, "windrunner");
	EXPECT_EQ(stos.find("malfurion")->second, "stormrage");
	EXPECT_EQ(stos.find("grommash")->second, "hellscream");
	EXPECT_EQ(stos.find("thrall"), stos.end());
}

TEST(FlatMap, LowerBound) {
	flat_map<std::string, int> stoi;

	stoi["heimerdinger"] = 8;
	stoi["miss fortune"] = 2;
	stoi["diana"] = 15;
	stoi["evelyn"] = 3;
	stoi["lissandra"] = 5;
	stoi["fiora"] = 2;
	stoi["gnar"] = 8;
	stoi["braum"] = 7;
	stoi["cassiopeia"] = 9;
	stoi["annie"] = 4;
	stoi["irelia"] = 9;
	stoi["jayce"] = 2;

	auto lb = stoi.lower_bound("irelia");
	EXPECT_EQ(lb->first, "irelia");

	lb = stoi.lower_bound("izaaaa");
	EXPECT_EQ(lb->second, 2);

}

TEST(FlatMap, FindNth) {
	std::vector<int> value(100);

	for(int i = 0; i < 100; ++i) {
		value[i] = (rand() % 1000) - 500;
	}

	flat_map<int, int> m;
	for(int i = 0; i < 100; ++i) {
		m[i] = value[i];
	}

	for(int i = 0; i < 100; ++i) {
		EXPECT_EQ((*m.find_nth(i)).second, value[i]);
	}
}
