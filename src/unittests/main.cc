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

#include <signal.h>
#include <gtest/gtest.h>

#include "common/crc.h"
#include "common/mfserr.h"
#include "common/random.h"
#include "common/sockets.h"

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	rnd_init();
	mycrc32_init();
	socketinit();
#ifndef _WIN32
	signal(SIGPIPE, SIG_IGN);
#endif
	return RUN_ALL_TESTS();
}
