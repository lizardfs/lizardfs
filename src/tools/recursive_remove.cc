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
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <thread>

#include "common/lambda_guard.h"
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
	uint32_t msgid = 0, job_id;

	sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, SIGINT);
	sigaddset(&set, SIGTERM);
	sigaddset(&set, SIGHUP);
	sigaddset(&set, SIGUSR1);
	sigprocmask(SIG_BLOCK, &set, NULL);

	if (realpath(file_name, path_buf) == nullptr) {
		printf("%s: Resolving path returned error\n", file_name);
		return -1;
	}
	std::string parent_path(path_buf);
	parent_path = parent_path.substr(0, parent_path.find_last_of("/"));

	fd = open_master_conn(parent_path.c_str(), &parent, nullptr, false);
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
		auto request = cltoma::requestTaskId::build(msgid);
		auto response = ServerConnection::sendAndReceive(fd,
				request, LIZ_MATOCL_REQUEST_TASK_ID,
				ServerConnection::ReceiveMode::kReceiveFirstNonNopMessage,
				long_wait ? kInfiniteTimeout : kDefaultTimeout);
		matocl::requestTaskId::deserialize(response, msgid, job_id);

		std::thread signal_thread(std::bind(signalHandler, job_id));

		/* destructor of LambdaGuard will send SIGUSR1 signal in order to
		 * return from signalHandler function and join thread */
		auto join_guard = makeLambdaGuard([&signal_thread]() {
			kill(getpid(), SIGUSR1);
			signal_thread.join();
		});
		request = cltoma::recursiveRemove::build(msgid, job_id, parent, fname, uid, gid);
		response = ServerConnection::sendAndReceive(fd, request, LIZ_MATOCL_RECURSIVE_REMOVE,
					ServerConnection::ReceiveMode::kReceiveFirstNonNopMessage,
					long_wait ? kInfiniteTimeout : kDefaultTimeout);

		matocl::recursiveRemove::deserialize(response, msgid, status);

		close_master_conn(0);

		if (status == LIZARDFS_STATUS_OK) {
			printf("Recursive remove (%s) completed\n", path_buf);
			return 0;
		} else {
			printf("Recursive remove (%s):\n returned error status %d: %s\n", path_buf, status, lizardfs_error_string(status));
			return -1;
		}

	} catch (Exception &e) {
		fprintf(stderr, "%s\n", e.what());
		close_master_conn(1);
		return -1;
	}
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
