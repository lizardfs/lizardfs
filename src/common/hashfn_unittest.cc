#include "common/platform.h"
#include "common/hashfn.h"

#include <algorithm>
#include <gtest/gtest.h>

TEST(HashTests, StringHashTest) {
	std::string s1 = "foo bar";
	std::string s2 = "";
	std::string s3 = "foo bas";
	uint64_t h1 = hash(ByteArray(s1.data(), s1.length()));
	uint64_t h2 = hash(ByteArray(s2.data(), s2.length()));
	uint64_t h3 = hash(ByteArray(s3.data(), s3.length()));
	EXPECT_NE(h1, h2);
	EXPECT_NE(h1, h3);
	EXPECT_NE(h2, h3);
}

TEST(HashTests, CombineTest) {
	uint64_t h1 = hash(13);
	uint64_t h2 = hash(~13);
	uint64_t h3 = 0;
	uint64_t seed1 = 0x1234;
	uint64_t seed2 = ~0;

	hashCombineRaw(seed1, h1);
	hashCombineRaw(seed1, h2);
	hashCombineRaw(seed2, h1);
	hashCombineRaw(seed2, h2);
	EXPECT_NE(seed1, seed2);

	seed1 = seed2 = 0x1234;
	hashCombineRaw(seed1, h1);
	hashCombineRaw(seed1, h2);
	hashCombineRaw(seed2, h2);
	hashCombineRaw(seed2, h1);
	EXPECT_NE(seed1, seed2);

	uint64_t seed1a = 0x1234;
	uint64_t seed1b = ~0;
	hashCombineRaw(seed1a, h1);
	hashCombineRaw(seed1a, h3);
	hashCombineRaw(seed1b, h1);
	EXPECT_NE(seed1a, seed1b);
}

TEST(HashTests, PrimitivesHashTest) {
	// make a vector of a 1M hashes and check if all are different
	const int kHashesSize = 1000000;
	std::vector<uint64_t> hashes;
	for (int i = 0; i < kHashesSize; ++i) {
		hashes.push_back(hash(i));
	}
	std::sort(hashes.begin(), hashes.end());
	EXPECT_EQ(std::unique(hashes.begin(), hashes.end()) - hashes.begin(), kHashesSize);

	// same with 32 bits variables
	hashes.clear();
	for (int i = 0; i < kHashesSize; ++i) {
		hashes.push_back(hash(i));
	}
	std::sort(hashes.begin(), hashes.end());
	EXPECT_EQ(std::unique(hashes.begin(), hashes.end()) - hashes.begin(), kHashesSize);

	hashes = std::vector<uint64_t>{
			hash(static_cast<bool>(1)),
			hash(static_cast<char>(2)),
			hash(static_cast<signed char>(3)),
			hash(static_cast<unsigned char>(4)),
			hash(static_cast<short>(5)),
			hash(static_cast<unsigned short>(6)),
			hash(static_cast<int>(7)),
			hash(static_cast<unsigned int>(8)),
			hash(static_cast<long>(9)),
			hash(static_cast<unsigned long>(10)),
			hash(static_cast<long long>(11)),
			hash(static_cast<unsigned long long>(12))};

	std::sort(hashes.begin(), hashes.end());
	EXPECT_EQ(std::unique(hashes.begin(), hashes.end()) - hashes.begin(), 12);

	hashes.push_back(hash(static_cast<uint64_t>(12)));
	std::sort(hashes.begin(), hashes.end());
	EXPECT_EQ(std::unique(hashes.begin(), hashes.end()) - hashes.begin(), 12);

	hashes.push_back(hash(static_cast<char>(12)));
	std::sort(hashes.begin(), hashes.end());
	EXPECT_EQ(std::unique(hashes.begin(), hashes.end()) - hashes.begin(), 13);
}

TEST(HashTests, VariadicHashCombineTest) {
	const uint64_t initSeed = 12345;
	const std::string str("Lenna");
	uint64_t seed1 = initSeed, seed2 = initSeed;

	hashCombine(seed1, static_cast<bool>(1));
	hashCombine(seed1, static_cast<char>(2));
	hashCombine(seed1, static_cast<signed char>(3));
	hashCombine(seed1, static_cast<unsigned char>(4));
	hashCombine(seed1, static_cast<short>(5));
	hashCombine(seed1, static_cast<unsigned short>(6));
	hashCombine(seed1, static_cast<int>(7));
	hashCombine(seed1, static_cast<unsigned int>(8));
	hashCombine(seed1, static_cast<long>(9));
	hashCombine(seed1, static_cast<unsigned long>(10));
	hashCombine(seed1, static_cast<long long>(11));
	hashCombine(seed1, static_cast<unsigned long long>(12));
	hashCombineRaw(seed1, hash(ByteArray(str.data(), str.length())));

	hashCombine(
			seed2,
			static_cast<bool>(1),
			static_cast<char>(2),
			static_cast<signed char>(3),
			static_cast<unsigned char>(4),
			static_cast<short>(5),
			static_cast<unsigned short>(6),
			static_cast<int>(7),
			static_cast<unsigned int>(8),
			static_cast<long>(9),
			static_cast<unsigned long>(10),
			static_cast<long long>(11),
			static_cast<unsigned long long>(12),
			ByteArray(str.data(), str.length()));
	EXPECT_EQ(seed1, seed2);
}
