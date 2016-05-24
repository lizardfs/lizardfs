/*
   Copyright 2016-2017 Skytechnology sp. z o.o.

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

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>

#include "common/server_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static int kDefaultTimeout = 60 * 1000;              // default timeout (60 seconds)
static int kInfiniteTimeout = 10 * 24 * 3600 * 1000; // simulate infinite timeout (10 days)

static void recursive_remove_usage() {
	fprintf(stderr,
	        "recursive remove\n\nusage:\n lizardfs rremove [-l] name [name ...]\n");
	fprintf(stderr, " -l - wait until removing will finish (otherwise there is %ds timeout)\n",
		kDefaultTimeout/1000);
}

static int recursive_remove(const char *file_name, int long_wait) {
	char path_buf[PATH_MAX];
	uint32_t parent, uid, gid;
	int fd;
	uint8_t status;
	uint32_t msgid = 0;
	if (realpath(file_name, path_buf) == nullptr) {
		printf("%s: Resolving path returned error\n", file_name);
		return -1;
	}
	std::string parent_path(path_buf);
	parent_path = parent_path.substr(0, parent_path.find_last_of("/"));

	fd = open_master_conn(parent_path.c_str(), &parent, nullptr, 0, 0);

	if (fd < 0) {
		return -1;
	}

	uid = getuid();
	gid = getgid();

	std::string fname(path_buf);
	std::size_t pos = fname.find_last_of("/");
	if (pos != std::string::npos) {
		fname = fname.substr(pos + 1);
	}

	printf("Executing recursive remove (%s) ...\n", path_buf);
	try {
		auto request = cltoma::recursiveRemove::build(msgid, parent, fname, uid, gid);
		auto response = ServerConnection::sendAndReceive(fd, request, LIZ_MATOCL_RECURSIVE_REMOVE,
					ServerConnection::ReceiveMode::kReceiveFirstNonNopMessage,
					long_wait ? kInfiniteTimeout : kDefaultTimeout);
		matocl::recursiveRemove::deserialize(response, msgid, status);
	} catch (Exception &e) {
		fprintf(stderr, "%s\n", e.what());
		close_master_conn(1);
		return -1;
	}
	if (status == LIZARDFS_STATUS_OK) {
		printf("Recursive remove (%s) completed\n", path_buf);
	} else {
		printf("Recursive remove (%s) returned error status %d: %s\n", path_buf, status, mfsstrerr(status));
	}

	close_master_conn(0);
	return 0;
}

int recursive_remove_run(int argc, char **argv) {
	char ch;
	int status;
	int long_wait = 0;

	while ((ch = getopt(argc, argv, "l")) != -1) {
		switch(ch) {
			case 'l':
				long_wait = 1;
				break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc < 1) {
		recursive_remove_usage();
		return 1;
	}

	status = 0;
	while (argc > 0) {
		if (recursive_remove(*argv, long_wait) < 0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}
