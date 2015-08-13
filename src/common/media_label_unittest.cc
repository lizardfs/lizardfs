/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
#include "common/media_label.h"

#include <gtest/gtest.h>

TEST(MediaLabelManagerTests, IsLabelValid) {
	EXPECT_TRUE(MediaLabelManager::isLabelValid(MediaLabelManager::kWildcard));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("QWERTYUIOPASDFGHJKLZXCVBNM"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("qwertyuiopasdfghjklzxcvbnm_"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("0987654321"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("l"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("L"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("0"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("_"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("lL_0"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("8834874543"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("llkcnlqwxne"));
	EXPECT_TRUE(MediaLabelManager::isLabelValid("ll_kcnl_qwx_ne"));
	EXPECT_TRUE(
	        MediaLabelManager::isLabelValid("123456789012345678901234567890AB"));  // max size

	EXPECT_FALSE(
	        MediaLabelManager::isLabelValid("123456789012345678901234567890ABC"));  // too long
	EXPECT_FALSE(MediaLabelManager::isLabelValid(""));
	EXPECT_FALSE(MediaLabelManager::isLabelValid(" "));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("hdd ssd"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("s/dasdasdas"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("sdas_fasfasgdasgsa-"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("hdd "));
	EXPECT_FALSE(MediaLabelManager::isLabelValid(" hdd"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("+"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("-"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("]"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("["));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("."));
	EXPECT_FALSE(MediaLabelManager::isLabelValid(";"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid(","));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("@"));
	EXPECT_FALSE(MediaLabelManager::isLabelValid("'"));
}

TEST(MediaLabelManagerTests, HandleTests) {
	MediaLabel wildcard(MediaLabel::kWildcard);
	MediaLabel handle1("value1");
	MediaLabel handle2("value2");
	MediaLabel handle3("value3");

	EXPECT_EQ(MediaLabel::kWildcard, MediaLabel(MediaLabelManager::kWildcardHandleValue));

	EXPECT_EQ(wildcard, MediaLabel(MediaLabelManager::kWildcard));
	EXPECT_EQ(handle1, MediaLabel("value1"));
	EXPECT_EQ(handle2, MediaLabel("value2"));
	EXPECT_EQ(handle3, MediaLabel("value3"));

	EXPECT_EQ((std::string)wildcard, MediaLabelManager::kWildcard);
	EXPECT_EQ((std::string)handle1, "value1");
	EXPECT_EQ((std::string)handle2, "value2");
	EXPECT_EQ((std::string)handle3, "value3");
}
