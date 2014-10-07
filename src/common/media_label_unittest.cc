#include "common/platform.h"
#include "common/media_label.h"

#include <gtest/gtest.h>

TEST(MEdiaLabelTests, IsMediaLabelValid) {
	EXPECT_TRUE(isMediaLabelValid(kMediaLabelWildcard));
	EXPECT_TRUE(isMediaLabelValid("QWERTYUIOPASDFGHJKLZXCVBNM"));
	EXPECT_TRUE(isMediaLabelValid("qwertyuiopasdfghjklzxcvbnm_0987654321"));
	EXPECT_TRUE(isMediaLabelValid("l"));
	EXPECT_TRUE(isMediaLabelValid("L"));
	EXPECT_TRUE(isMediaLabelValid("0"));
	EXPECT_TRUE(isMediaLabelValid("_"));
	EXPECT_TRUE(isMediaLabelValid("lL_0"));
	EXPECT_TRUE(isMediaLabelValid("8834874543"));
	EXPECT_TRUE(isMediaLabelValid("llkcnlqwxne"));
	EXPECT_TRUE(isMediaLabelValid("ll_kcnl_qwx_ne"));

	EXPECT_FALSE(isMediaLabelValid(""));
	EXPECT_FALSE(isMediaLabelValid(" "));
	EXPECT_FALSE(isMediaLabelValid("hdd ssd"));
	EXPECT_FALSE(isMediaLabelValid("s/dasdasdas"));
	EXPECT_FALSE(isMediaLabelValid("sdas_fasfasgdasgsa-"));
	EXPECT_FALSE(isMediaLabelValid("hdd "));
	EXPECT_FALSE(isMediaLabelValid(" hdd"));
	EXPECT_FALSE(isMediaLabelValid("+"));
	EXPECT_FALSE(isMediaLabelValid("-"));
	EXPECT_FALSE(isMediaLabelValid("]"));
	EXPECT_FALSE(isMediaLabelValid("["));
	EXPECT_FALSE(isMediaLabelValid("."));
	EXPECT_FALSE(isMediaLabelValid(";"));
	EXPECT_FALSE(isMediaLabelValid(","));
	EXPECT_FALSE(isMediaLabelValid("@"));
	EXPECT_FALSE(isMediaLabelValid("'"));
}
