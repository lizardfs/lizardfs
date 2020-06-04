#include "common/platform.h"

#ifdef LIZARDFS_HAVE_DB
#include "master/hstring_bdbstorage.h"
#endif

#include "master/hstring_memstorage.h"

#include <functional>
#include <gtest/gtest.h>

using namespace hstorage;

TEST(HStringTest, Name) {
	Storage::reset(new MemStorage());
	EXPECT_EQ(Storage::instance().name(), "MemStorage");
}

/*
 * MemStorage tests
 */
TEST(HStringTest, MemComparison) {
	Storage::reset(new MemStorage());
	HString str1("Good morning");
	HString str2("Good evening");
	Handle handle(str1);

	EXPECT_TRUE(str1 == handle);
	EXPECT_TRUE(handle == str1);
	EXPECT_FALSE(str1 != handle);
	EXPECT_FALSE(handle != str1);
	EXPECT_FALSE(str2 == handle);
	EXPECT_FALSE(handle == str2);

	EXPECT_TRUE(str1 >= handle);
	EXPECT_TRUE(handle <= str1);
	EXPECT_TRUE(str2 < handle);
	EXPECT_TRUE(handle > str2);
	EXPECT_TRUE(str1 <= handle);
	EXPECT_TRUE(handle >= str1);
	EXPECT_TRUE(handle > str2);
	EXPECT_TRUE(str2 < handle);

	EXPECT_TRUE(str1 > str2);
	EXPECT_TRUE(str1 >= str2);
	EXPECT_TRUE(str2 < str1);
	EXPECT_TRUE(str2 <= str1);

	EXPECT_TRUE(str1 >= str1);
	EXPECT_TRUE(str1 <= str1);
	EXPECT_TRUE(str2 >= str2);
	EXPECT_TRUE(str2 <= str2);
}

TEST(HStringTest, MemGet) {
	Storage::reset(new MemStorage());
	HString strs[]{HString("Good morning"), HString("Good evening"), HString()};
	for (auto &str : strs) {
		Handle handle(str);
		EXPECT_TRUE(str == handle);
	}
	Handle handle(strs[0] + strs[1] + strs[2]);
	EXPECT_TRUE(strs[0] + strs[1] + strs[2] == handle.get());
}

TEST(HStringTest, MemHash) {
	Storage::reset(new MemStorage());
	HString str("Good morning");
	Handle h1(str);
	Handle h2(str);

	EXPECT_EQ(MemStorage::hash(h1), MemStorage::hash(h2));
	EXPECT_EQ(MemStorage::hash(h1), static_cast<MemStorage::HashType>(::std::hash< ::std::string>()(str)));
	EXPECT_EQ(MemStorage::hash(h1), h1.hash());
}

TEST(HStringTest, MemCopy) {
	Storage::reset(new MemStorage());
	Handle h1("Good morning");
	Handle h2("Good evening");
	Handle h3;
	Handle h4 = h1;
	Handle h5(h2);

	h3 = std::move(h1);
	h1 = h2;
	h2 = std::move(h3);

	EXPECT_TRUE(h1 == h5.get());
	EXPECT_TRUE(h2 == h4.get());
}

/*
 * BDBStorage tests
 */
#ifdef LIZARDFS_HAVE_DB
TEST(HStringTest, BDBComparison) {
	const char DB_FILEPATH[] = "/tmp/db.db";
	{
		Storage::reset(new BDBStorage(DB_FILEPATH, 1024*1024, 1));
		HString str1("Good morning");
		HString str2("Good evening");
		Handle handle(str1);

		EXPECT_TRUE(str1 == handle);
		EXPECT_TRUE(handle == str1);
		EXPECT_FALSE(str1 != handle);
		EXPECT_FALSE(handle != str1);
		EXPECT_FALSE(str2 == handle);
		EXPECT_FALSE(handle == str2);

		EXPECT_TRUE(str1 >= handle);
		EXPECT_TRUE(handle <= str1);
		EXPECT_TRUE(str2 < handle);
		EXPECT_TRUE(handle > str2);
		EXPECT_TRUE(str1 <= handle);
		EXPECT_TRUE(handle >= str1);
		EXPECT_TRUE(handle > str2);
		EXPECT_TRUE(str2 < handle);

		EXPECT_TRUE(str1 > str2);
		EXPECT_TRUE(str1 >= str2);
		EXPECT_TRUE(str2 < str1);
		EXPECT_TRUE(str2 <= str1);

		EXPECT_TRUE(str1 >= str1);
		EXPECT_TRUE(str1 <= str1);
		EXPECT_TRUE(str2 >= str2);
		EXPECT_TRUE(str2 <= str2);
	} // destroy all Handle objects
	Storage::reset();
	std::remove(DB_FILEPATH);
}

TEST(HStringTest, BDBGet) {
	const char DB_FILEPATH[] = "/tmp/db.db";
	{
		Storage::reset(new BDBStorage(DB_FILEPATH, 1024*1024, 2));
		HString strs[]{HString("Good morning"), HString("Good evening"), HString()};
		for (auto &str : strs) {
			Handle handle(str);
			EXPECT_TRUE(str == handle);
		}
		Handle handle(strs[0] + strs[1] + strs[2]);
		EXPECT_TRUE(strs[0] + strs[1] + strs[2] == handle.get());
	} // destroy all Handle objects
	Storage::reset();
	std::remove(DB_FILEPATH);
}

TEST(HStringTest, BDBHash) {
	const char DB_FILEPATH[] = "/tmp/db.db";
	{
		Storage::reset(new BDBStorage(DB_FILEPATH, 1024*1024, 3));
		HString str("Good morning");
		Handle h1(str);
		Handle h2(str);

		EXPECT_EQ(BDBStorage::hash(h1), BDBStorage::hash(h2));
		EXPECT_EQ(BDBStorage::hash(h1), static_cast<BDBStorage::HashType>(::std::hash< ::std::string>()(str)));
		EXPECT_EQ(BDBStorage::hash(h1), h1.hash());
	} // destroy all Handle objects
	Storage::reset();
	std::remove(DB_FILEPATH);
}

TEST(HStringTest, BDBCopy) {
	Storage::reset(new MemStorage());
	Handle h1("Good morning");
	Handle h2("Good evening");
	Handle h3;
	Handle h4 = h1;
	Handle h5(h2);

	h3 = std::move(h1);
	h1 = h2;
	h2 = std::move(h3);

	EXPECT_TRUE(h1 == h5.get());
	EXPECT_TRUE(h2 == h4.get());
}
#endif
