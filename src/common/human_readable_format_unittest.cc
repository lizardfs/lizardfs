#include "config.h"
#include "common/human_readable_format.h"

#include <gtest/gtest.h>

TEST(HumanReadableFormatTests, ConvertToSi) {
	EXPECT_EQ("0", convertToSi(0));
	EXPECT_EQ("1", convertToSi(1));
	EXPECT_EQ("999", convertToSi(999));
	EXPECT_EQ("1.0k", convertToSi(1000));
	EXPECT_EQ("1.0k", convertToSi(1001));
	EXPECT_EQ("1.4k", convertToSi(1400));
	EXPECT_EQ("1.6k", convertToSi(1550));

	EXPECT_EQ("1.0M", convertToSi(1000000ULL));
	EXPECT_EQ("1.0G", convertToSi(1000000000ULL));
	EXPECT_EQ("1.0T", convertToSi(1000000000000ULL));
	EXPECT_EQ("1.0P", convertToSi(1000000000000000ULL));
	EXPECT_EQ("1.0E", convertToSi(1000000000000000000ULL));
	EXPECT_EQ("10E", convertToSi(10000000000000000000ULL));

	EXPECT_EQ("18E", convertToSi(18446744073709551615ULL));
}

TEST(HumanReadableFormatTests, ConvertToIec) {
	EXPECT_EQ("0", convertToIec(0));
	EXPECT_EQ("1", convertToIec(1));

	EXPECT_EQ("1000", convertToIec(1000));
	EXPECT_EQ("1.5Ki", convertToIec(1550));

	EXPECT_EQ("1023", convertToIec(1023));
	EXPECT_EQ("1.0Ki", convertToIec(1024));
	EXPECT_EQ("1.0Ki", convertToIec(1025));
	EXPECT_EQ("1023Ki", convertToIec(1023 * 1024));
	EXPECT_EQ("1023Pi", convertToIec(1023ULL * 1024 * 1024 * 1024 * 1024 * 1024));

	EXPECT_EQ("888Pi", convertToIec(1000000000000000000ULL));
	EXPECT_EQ("8.7Ei", convertToIec(10000000000000000000ULL));

	EXPECT_EQ("1.0Mi", convertToIec(1024ULL * 1024));
	EXPECT_EQ("1.0Gi", convertToIec(1024ULL * 1024 * 1024));
	EXPECT_EQ("1.0Ti", convertToIec(1024ULL * 1024 * 1024 * 1024));
	EXPECT_EQ("1.0Pi", convertToIec(1024ULL * 1024 * 1024 * 1024 * 1024));
	EXPECT_EQ("1.0Ei", convertToIec(1024ULL * 1024 * 1024 * 1024 * 1024 * 1024));

	EXPECT_EQ("16Ei", convertToIec(18446744073709551615ULL));
}

