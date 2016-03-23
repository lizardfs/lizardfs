/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2016 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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

#include <stdio.h>
#include <stdlib.h>

#include "tools/tools_commands.h"

int set_goal(const char *fname, const std::string &goal, uint8_t mode) {
	uint32_t inode;
	int fd;
	uint32_t messageId = 0;
	uint32_t uid = getuid();
	fd = open_master_conn(fname, &inode, NULL, 0, 1);
	if (fd < 0) {
		return -1;
	}
	try {
		auto request = cltoma::fuseSetGoal::build(messageId, inode, uid, goal, mode);
		auto response = ServerConnection::sendAndReceive(fd, request, LIZ_MATOCL_FUSE_SETGOAL);
		uint32_t changed;
		uint32_t notChanged;
		uint32_t notPermitted;
		PacketVersion version;

		deserializePacketVersionNoHeader(response, version);
		if (version == matocl::fuseSetGoal::kStatusPacketVersion) {
			uint8_t status;
			matocl::fuseSetGoal::deserialize(response, messageId, status);
			throw Exception(std::string(fname) + ": failed", status);
		}
		matocl::fuseSetGoal::deserialize(response, messageId, changed, notChanged, notPermitted);

		if ((mode & SMODE_RMASK) == 0) {
			if (changed || mode == SMODE_SET) {
				printf("%s: %s\n", fname, goal.c_str());
			} else {
				printf("%s: goal not changed\n", fname);
			}
		} else {
			printf("%s:\n", fname);
			print_number(" inodes with goal changed:      ", "\n", changed, 1, 0, 1);
			print_number(" inodes with goal not changed:  ", "\n", notChanged, 1, 0, 1);
			print_number(" inodes with permission denied: ", "\n", notPermitted, 1, 0, 1);
		}
	} catch (Exception &e) {
		fprintf(stderr, "%s\n", e.what());
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	return 0;
}
