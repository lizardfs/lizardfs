#include "admin/escape_porcelain_string.h"
#include "common/platform.h"

#include <gtest/gtest.h>

TEST(EscapePorcelainStringTests, EscapePorcelainString) {
	EXPECT_EQ(R"("")", escapePorcelainString(R"()"));
	EXPECT_EQ(R"(lubie)", escapePorcelainString(R"(lubie)"));
	EXPECT_EQ(R"(1*_,2*hdd)", escapePorcelainString(R"(1*_,2*hdd)"));
	EXPECT_EQ(R"("\\")", escapePorcelainString(R"(\)"));
	EXPECT_EQ(R"("\"")", escapePorcelainString(R"(")"));
	EXPECT_EQ(R"("\"\"")", escapePorcelainString(R"("")"));
	EXPECT_EQ(R"("lubie placuszki")", escapePorcelainString(R"(lubie placuszki)"));
	EXPECT_EQ(R"("lubie\\placuszki")", escapePorcelainString(R"(lubie\placuszki)"));
	EXPECT_EQ(R"("lubie\\\\placuszki")", escapePorcelainString(R"(lubie\\placuszki)"));
	EXPECT_EQ(R"("lubie\\\\\\placuszki")", escapePorcelainString(R"(lubie\\\placuszki)"));
	EXPECT_EQ(R"("\\lubie\\placuszki\\")", escapePorcelainString(R"(\lubie\placuszki\)"));
	EXPECT_EQ(R"("\"lubie\\placuszki\"")", escapePorcelainString(R"("lubie\placuszki")"));
	EXPECT_EQ(R"("lubie\\ placuszki")", escapePorcelainString(R"(lubie\ placuszki)"));
	EXPECT_EQ(R"("\"lubie\\ placuszki\"")", escapePorcelainString(R"("lubie\ placuszki")"));
}
