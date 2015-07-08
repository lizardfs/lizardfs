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

TEST(MediaLabelTests, IsMediaLabelValid) {
	EXPECT_TRUE(isMediaLabelValid(kMediaLabelWildcard));
	EXPECT_TRUE(isMediaLabelValid("QWERTYUIOPASDFGHJKLZXCVBNM"));
	EXPECT_TRUE(isMediaLabelValid("qwertyuiopasdfghjklzxcvbnm_"));
	EXPECT_TRUE(isMediaLabelValid("0987654321"));
	EXPECT_TRUE(isMediaLabelValid("l"));
	EXPECT_TRUE(isMediaLabelValid("L"));
	EXPECT_TRUE(isMediaLabelValid("0"));
	EXPECT_TRUE(isMediaLabelValid("_"));
	EXPECT_TRUE(isMediaLabelValid("lL_0"));
	EXPECT_TRUE(isMediaLabelValid("8834874543"));
	EXPECT_TRUE(isMediaLabelValid("llkcnlqwxne"));
	EXPECT_TRUE(isMediaLabelValid("ll_kcnl_qwx_ne"));
	EXPECT_TRUE(isMediaLabelValid("123456789012345678901234567890AB")); // max size

	EXPECT_FALSE(isMediaLabelValid("123456789012345678901234567890ABC")); // too long
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
