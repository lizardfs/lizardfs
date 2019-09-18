/*
   Copyright 2017 Skytechnology sp. z o.o.

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
#include "mount/direntry_cache.h"

#include <gtest/gtest.h>
#include <iostream>

TEST(DirEntryCache, Basic) {
	DirEntryCache cache(5000000);

	Attributes dummy_attributes;
	dummy_attributes.fill(0);
	Attributes attributes_with_6 = dummy_attributes;
	Attributes attributes_with_9 = dummy_attributes;
	attributes_with_6[0] = 6;
	attributes_with_9[0] = 9;
	auto current_time = cache.updateTime();
	cache.insertSubsequent(LizardClient::Context(0, 0, 0, 0), 9, 0, std::vector<DirectoryEntry>{{7, 0, 1, "a1", dummy_attributes}, {8, 1, 2, "a2", dummy_attributes}, {9, 2, 3, "a3", dummy_attributes}}, current_time);
	cache.insertSubsequent(LizardClient::Context(1, 2, 0, 0), 11, 7, std::vector<DirectoryEntry>{{5, 7, 8, "a1", dummy_attributes}, {4, 8, 9, "a2", dummy_attributes}, {3, 9, 10, "a3", dummy_attributes}}, current_time);
	cache.insertSubsequent(LizardClient::Context(0, 0, 0, 0), 9, 1, std::vector<DirectoryEntry>{{11, 1, 2, "a4", dummy_attributes}, {13, 2, 3, "a3", attributes_with_9}, {12, 3, 4, "a2", attributes_with_6}}, current_time);

	std::vector<std::tuple<int, int, int, std::string>> index_output {
		std::make_tuple(7, 9, 0, "a1"),
		std::make_tuple(11, 9, 1, "a4"),
		std::make_tuple(13, 9, 2, "a3"),
		std::make_tuple(12, 9, 3, "a2"),
		std::make_tuple(5, 11, 7, "a1"),
		std::make_tuple(4, 11, 8, "a2"),
		std::make_tuple(3, 11, 9, "a3")
	};

	std::vector<std::tuple<int, int, int, std::string>> lookup_output {
		std::make_tuple(7, 9, 0, "a1"),
		std::make_tuple(12, 9, 3, "a2"),
		std::make_tuple(13, 9, 2, "a3"),
		std::make_tuple(11, 9, 1, "a4"),
		std::make_tuple(5, 11, 7, "a1"),
		std::make_tuple(4, 11, 8, "a2"),
		std::make_tuple(3, 11, 9, "a3")
	};

	auto index_it = cache.index_begin();
	auto index_output_it = index_output.begin();
	ASSERT_EQ(cache.size(), index_output.size());
	while (index_it != cache.index_end()) {
		ASSERT_EQ(*index_output_it, std::make_tuple(index_it->inode, index_it->parent_inode, index_it->index, index_it->name));
		index_it++;
		index_output_it++;
	}

	auto lookup_it = cache.lookup_begin();
	auto lookup_output_it = lookup_output.begin();
	ASSERT_EQ(cache.size(), lookup_output.size());
	while (lookup_it != cache.lookup_end()) {
		ASSERT_EQ(*lookup_output_it, std::make_tuple(lookup_it->inode, lookup_it->parent_inode, lookup_it->index, lookup_it->name));
		lookup_it++;
		lookup_output_it++;
	}

	auto by_inode_it = cache.find(LizardClient::Context(0, 0, 0, 0), 12);
	ASSERT_NE(by_inode_it, cache.inode_end());
	ASSERT_EQ(by_inode_it->attr[0], 6);
	by_inode_it++;
	ASSERT_NE(by_inode_it, cache.inode_end());
	ASSERT_EQ(by_inode_it->attr[0], 9);
	by_inode_it++;
	ASSERT_EQ(by_inode_it, cache.inode_end());
}

TEST(DirEntryCache, Repetitions) {
	DirEntryCache cache(5000000);

	Attributes dummy_attributes;
	dummy_attributes.fill(0);
	auto current_time = cache.updateTime();

	cache.insertSubsequent(LizardClient::Context(0, 0, 0, 0), 9, 0, std::vector<DirectoryEntry>{{7, 0, 1, "a1", dummy_attributes}}, current_time);
	cache.insertSubsequent(LizardClient::Context(0, 0, 0, 0), 9, 1, std::vector<DirectoryEntry>{{7, 1, 2, "a1", dummy_attributes}}, current_time);
	cache.removeOldest(5);
}
