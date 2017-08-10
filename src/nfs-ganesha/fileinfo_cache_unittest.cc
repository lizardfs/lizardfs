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

#include "fileinfo_cache.h"

#include <gtest/gtest.h>

TEST(FileInfoCache, Basic) {
	liz_fileinfo_cache_t *cache = liz_create_fileinfo_cache(16, 0);

	liz_fileinfo_entry_t *entry7 = liz_fileinfo_cache_acquire(cache, 7);
	liz_fileinfo_entry_t *entry11 = liz_fileinfo_cache_acquire(cache, 11);
	ASSERT_EQ(liz_extract_fileinfo(entry7), nullptr);
	ASSERT_EQ(liz_extract_fileinfo(entry11), nullptr);

	liz_attach_fileinfo(entry7, (liz_fileinfo_t *)0xb00b1e5);
	liz_attach_fileinfo(entry11, (liz_fileinfo_t *)0xface);

	liz_fileinfo_entry_t *expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_EQ(expired, nullptr);
	liz_fileinfo_cache_release(cache, entry7);

	liz_fileinfo_entry_t *entry7_2 = liz_fileinfo_cache_acquire(cache, 7);
	ASSERT_NE(liz_extract_fileinfo(entry7_2), nullptr);
	ASSERT_EQ(liz_extract_fileinfo(entry7_2), (liz_fileinfo_t *)0xb00b1e5);
	ASSERT_EQ(liz_extract_fileinfo(entry11), (liz_fileinfo_t *)0xface);

	expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_EQ(expired, nullptr);

	liz_fileinfo_cache_release(cache, entry7_2);

	expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_NE(expired, nullptr);
	liz_fileinfo_entry_free(expired);

	liz_fileinfo_cache_release(cache, entry11);
	expired = liz_fileinfo_cache_pop_expired(cache);
	while (expired) {
		liz_fileinfo_entry_free(expired);
		expired = liz_fileinfo_cache_pop_expired(cache);
	}

	liz_destroy_fileinfo_cache(cache);
}

TEST(FileInfoCache, Full) {
	liz_fileinfo_cache_t *cache = liz_create_fileinfo_cache(3, 0);

	auto a1 = liz_fileinfo_cache_acquire(cache, 1);
	auto a2 = liz_fileinfo_cache_acquire(cache, 1);
	auto a3 = liz_fileinfo_cache_acquire(cache, 1);
	auto a4 = liz_fileinfo_cache_acquire(cache, 1);

	ASSERT_NE(a1, nullptr);
	ASSERT_NE(a2, nullptr);
	ASSERT_NE(a3, nullptr);
	ASSERT_NE(a4, nullptr);

	liz_fileinfo_cache_erase(cache, a1);
	liz_fileinfo_cache_release(cache, a2);
	liz_fileinfo_cache_release(cache, a3);
	liz_fileinfo_cache_release(cache, a4);

	auto expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_NE(expired, nullptr);
	liz_fileinfo_entry_free(expired);

	expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_NE(expired, nullptr);
	liz_fileinfo_entry_free(expired);

	expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_NE(expired, nullptr);
	liz_fileinfo_entry_free(expired);

	expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_EQ(expired, nullptr);

	liz_destroy_fileinfo_cache(cache);
}

TEST(FileInfoCache, Reset) {
	liz_fileinfo_cache_t *cache = liz_create_fileinfo_cache(100000, 100000);

	auto a1 = liz_fileinfo_cache_acquire(cache, 1);
	liz_fileinfo_cache_release(cache, a1);

	auto expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_EQ(expired, nullptr);

	liz_reset_fileinfo_cache_params(cache, 0, 0);

	expired = liz_fileinfo_cache_pop_expired(cache);
	ASSERT_NE(expired, nullptr);
	liz_fileinfo_entry_free(expired);

	liz_destroy_fileinfo_cache(cache);
}
