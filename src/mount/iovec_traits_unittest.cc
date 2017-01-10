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
#include "mount/client/iovec_traits.h"

#include <gtest/gtest.h>

TEST(IoVecTraits, MemcpyAndCopy) {
	char buf[] = "abcdefghijklmnopqrstuvwxyz";
	char out[512] = {0};
	std::vector<struct iovec> iov{{out, 1}, {out + 1, 2}, {out, 0}, {out + 1, 0}, {out + 3, 3}, {out + 6, 11}, {nullptr, 0}};
	memcpyIoVec(iov.data(), iov.size(), buf, 9);

	ASSERT_EQ(std::string(out), "abcdefghi");

	std::vector<struct iovec> iov2{{out + 128, 2}, {out + 130, 1}, {nullptr, 0}, {out + 131, 4}, {out + 135, 16}};
	ssize_t ret = copyIoVec(iov2.data(), iov2.size(), iov.data(), iov.size());
	ASSERT_EQ(ret, 17);
	ASSERT_EQ(std::string(out + 128), std::string(out));
}
