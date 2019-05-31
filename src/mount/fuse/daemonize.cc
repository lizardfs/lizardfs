/*
   Copyright 2019 Skytechnology sp. z o.o.

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

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int gWaiter[2] = {-1, -1};

void daemonize_return_status(int status) {
	if (gWaiter[1] < 0)
		return;

	if (write(gWaiter[1], &status, sizeof(int)) != sizeof(int))
		fprintf(stderr, "pipe write error: %s", strerror(errno));

	close(gWaiter[1]);
	gWaiter[1] = -1;
}

int daemonize_and_wait(std::function<int()> run_function) {
	gWaiter[0] = gWaiter[1] = -1;

	if (pipe(gWaiter) < 0) {
		fprintf(stderr, "pipe creation error: %s", strerror(errno));
		return 1;
	}

	int status;
	int child_pid = fork();

	if (child_pid < 0) {
		fprintf(stderr, "fork error: %s", strerror(errno));
		return 1;
	}

	if (child_pid > 0) {
		close(gWaiter[1]);
		int r = read(gWaiter[0], &status, sizeof(int));
		if (r != sizeof(int))
			status = 1;
		return status;
	}

	close(gWaiter[0]);
	status = run_function();
	daemonize_return_status(status);

	return status;
}
