/*
   Copyright 2018 Skytechnology sp. z o.o.

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
#include "daemonize.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int gWaiter[2] = {-1, -1};

void daemonize_return_status(int status) {
	if (gWaiter[1] >= 0) {
		if (write(gWaiter[1], &status, sizeof(int)) != sizeof(int)) {
			fprintf(stderr, "pipe write error: %s", strerror(errno));
		}
		close(gWaiter[1]);
		gWaiter[1] = -1;
	}
}

int daemonize_and_wait(bool block_output, std::function<int()> run_function) {
	gWaiter[0] = gWaiter[1] = -1;

	if (pipe(gWaiter) < 0) {
		fprintf(stderr, "pipe creation error: %s", strerror(errno));
		return 1;
	}

	int child_pid = fork();
	if (child_pid < 0) {
		fprintf(stderr, "fork error: %s", strerror(errno));
		return 1;
	} else if (child_pid > 0) {
		int status;
		close(gWaiter[1]);
		int r = read(gWaiter[0], &status, sizeof(int));
		if (r != sizeof(int)) {
			status = 1;
		}
		return status;
	}

	if (block_output) {
		setsid();
		setpgid(0, getpid());
		int nullfd = open("/dev/null", O_RDWR, 0);
		if (nullfd != -1) {
			(void)dup2(nullfd, 0);
			(void)dup2(nullfd, 1);
			(void)dup2(nullfd, 2);
			if (nullfd > 2) {
				close(nullfd);
			}
		}
	}

	close(gWaiter[0]);
	int status;
	status = run_function();
	daemonize_return_status(status);
	return status;
}
