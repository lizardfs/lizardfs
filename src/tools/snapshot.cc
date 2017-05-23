/*
   Copyright 2005-2017 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2017 Skytechnology sp. z o.o..

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

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "common/datapack.h"
#include "common/lambda_guard.h"
#include "common/moosefs_string.h"
#include "common/server_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static int kDefaultTimeout = 60 * 1000;              // default timeout (60 seconds)
static int kInfiniteTimeout = 10 * 24 * 3600 * 1000; // simulate infinite timeout (10 days)


static void snapshot_usage() {
	fprintf(stderr,
	        "make snapshot (lazy copy)\n\nusage:\n lizardfs makesnapshot [-ofl] src [src ...] dst\n");
	fprintf(stderr, " -o,-f - allow to overwrite existing objects\n");
	fprintf(stderr, " -l - wait until snapshot will finish (otherwise there is 60s timeout)\n");
}

static int make_snapshot(const char *dstdir, const char *dstbase, const char *srcname,
	                 uint32_t srcinode, uint8_t canoverwrite, int long_wait, uint8_t ignore_missing_src, int initial_batch_size) {
	uint32_t nleng, dstinode, uid, gid;
	uint8_t status;
	uint32_t msgid = 0, job_id;
	int fd;

	sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, SIGINT);
	sigaddset(&set, SIGTERM);
	sigaddset(&set, SIGHUP);
	sigaddset(&set, SIGUSR1);
	sigprocmask(SIG_BLOCK, &set, NULL);

	nleng = strlen(dstbase);
	if (nleng > 255) {
		printf("%s: name too long\n", dstbase);
		return -1;
	}

	fd = open_master_conn(dstdir, &dstinode, NULL, 0, 1);
	if (fd < 0) {
		return -1;
	}

	uid = getuid();
	gid = getgid();

	printf("Creating snapshot: %s -> %s/%s ...\n", srcname, dstdir, dstbase);
	try {
		auto request = cltoma::requestTaskId::build(msgid);
		auto response = ServerConnection::sendAndReceive(fd, request,
				LIZ_MATOCL_REQUEST_TASK_ID,
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
		request = cltoma::snapshot::build(msgid, job_id, srcinode, dstinode, MooseFsString<uint8_t>(dstbase),
		                                  uid, gid, canoverwrite, ignore_missing_src, initial_batch_size);
		response = ServerConnection::sendAndReceive(fd, request, LIZ_MATOCL_FUSE_SNAPSHOT,
				ServerConnection::ReceiveMode::kReceiveFirstNonNopMessage,
				long_wait ? kInfiniteTimeout : kDefaultTimeout);
		matocl::snapshot::deserialize(response, msgid, status);

		close_master_conn(0);

		if (status == LIZARDFS_STATUS_OK) {
			printf("Snapshot %s -> %s/%s completed\n", srcname, dstdir, dstbase);
			return 0;
		} else {
			printf("Snapshot %s -> %s/%s:\n returned error status %d: %s\n",
			       srcname, dstdir, dstbase, status, lizardfs_error_string(status));
			return -1;
		}

	} catch (Exception &e) {
		fprintf(stderr, "%s\n", e.what());
		close_master_conn(1);
		return -1;
	}
	return 0;
}

static int snapshot(const char *dstname, char *const *srcnames, uint32_t srcelements,
					uint8_t canowerwrite, int long_wait, uint8_t ignore_missing_src, int initial_batch_size) {
	char to[PATH_MAX + 1], base[PATH_MAX + 1], dir[PATH_MAX + 1];
	char src[PATH_MAX + 1];
	struct stat sst, dst;
	int status;
	uint32_t i, l;

	if (stat(dstname, &dst) < 0) {  // dst does not exist
		if (errno != ENOENT) {
			printf("%s: stat error: %s\n", dstname, strerr(errno));
			return -1;
		}
		if (srcelements > 1) {
			printf("can snapshot multiple elements only into existing directory\n");
			return -1;
		}
		if (lstat(srcnames[0], &sst) < 0) {
			printf("%s: lstat error: %s\n", srcnames[0], strerr(errno));
			return -1;
		}
		if (bsd_dirname(dstname, dir) < 0) {
			printf("%s: dirname error\n", dstname);
			return -1;
		}
		if (stat(dir, &dst) < 0) {
			printf("%s: stat error: %s\n", dir, strerr(errno));
			return -1;
		}
		if (sst.st_dev != dst.st_dev) {
			printf("(%s,%s): both elements must be on the same device\n", dstname, srcnames[0]);
			return -1;
		}
		if (realpath(dir, to) == NULL) {
			printf("%s: realpath error on %s: %s\n", dir, to, strerr(errno));
			return -1;
		}
		if (bsd_basename(dstname, base) < 0) {
			printf("%s: basename error\n", dstname);
			return -1;
		}
		if (strlen(dstname) > 0 && dstname[strlen(dstname) - 1] == '/' && !S_ISDIR(sst.st_mode)) {
			printf("directory %s does not exist\n", dstname);
			return -1;
		}
		return make_snapshot(to, base, srcnames[0], sst.st_ino, canowerwrite, long_wait, ignore_missing_src, initial_batch_size);
	} else {  // dst exists
		if (realpath(dstname, to) == NULL) {
			printf("%s: realpath error on %s: %s\n", dstname, to, strerr(errno));
			return -1;
		}
		if (!S_ISDIR(dst.st_mode)) {  // dst id not a directory
			if (srcelements > 1) {
				printf("can snapshot multiple elements only into existing directory\n");
				return -1;
			}
			if (lstat(srcnames[0], &sst) < 0) {
				printf("%s: lstat error: %s\n", srcnames[0], strerr(errno));
				return -1;
			}
			if (sst.st_dev != dst.st_dev) {
				printf("(%s,%s): both elements must be on the same device\n", dstname, srcnames[0]);
				return -1;
			}
			memcpy(dir, to, PATH_MAX + 1);
			dirname_inplace(dir);
			if (bsd_basename(to, base) < 0) {
				printf("%s: basename error\n", to);
				return -1;
			}
			return make_snapshot(dir, base, srcnames[0], sst.st_ino, canowerwrite, long_wait, ignore_missing_src, initial_batch_size);
		} else {  // dst is a directory
			status = 0;
			for (i = 0; i < srcelements; i++) {
				if (lstat(srcnames[i], &sst) < 0) {
					printf("%s: lstat error: %s\n", srcnames[i], strerr(errno));
					status = -1;
					continue;
				}
				if (sst.st_dev != dst.st_dev) {
					printf("(%s,%s): both elements must be on the same device\n", dstname,
					       srcnames[i]);
					status = -1;
					continue;
				}
				if (!S_ISDIR(sst.st_mode)) {      // src is not a directory
					if (!S_ISLNK(sst.st_mode)) {  // src is not a symbolic link
						if (realpath(srcnames[i], src) == NULL) {
							printf("%s: realpath error on %s: %s\n", srcnames[i], src,
							       strerr(errno));
							status = -1;
							continue;
						}
						if (bsd_basename(src, base) < 0) {
							printf("%s: basename error\n", src);
							status = -1;
							continue;
						}
					} else {  // src is a symbolic link
						if (bsd_basename(srcnames[i], base) < 0) {
							printf("%s: basename error\n", srcnames[i]);
							status = -1;
							continue;
						}
					}
					if (make_snapshot(to, base, srcnames[i], sst.st_ino, canowerwrite, long_wait,
					                  ignore_missing_src, initial_batch_size) < 0) {
						status = -1;
					}
				} else {  // src is a directory
					l = strlen(srcnames[i]);
					if (l > 0 &&
					    srcnames[i][l - 1] !=
					        '/') {  // src is a directory and name has trailing slash
						if (realpath(srcnames[i], src) == NULL) {
							printf("%s: realpath error on %s: %s\n", srcnames[i], src,
							       strerr(errno));
							status = -1;
							continue;
						}
						if (bsd_basename(src, base) < 0) {
							printf("%s: basename error\n", src);
							status = -1;
							continue;
						}
						if (make_snapshot(to, base, srcnames[i], sst.st_ino, canowerwrite,
						                  long_wait, ignore_missing_src, initial_batch_size) < 0) {
							status = -1;
						}
					} else {  // src is a directory and name has not trailing slash
						memcpy(dir, to, PATH_MAX + 1);
						dirname_inplace(dir);
						if (bsd_basename(to, base) < 0) {
							printf("%s: basename error\n", to);
							status = -1;
							continue;
						}
						if (make_snapshot(dir, base, srcnames[i], sst.st_ino, canowerwrite,
						                  long_wait, ignore_missing_src, initial_batch_size) < 0) {
							status = -1;
						}
					}
				}
			}
			return status;
		}
	}
}

int snapshot_run(int argc, char **argv) {
	int ch;
	int oflag = 0;
	int lflag = 0;
	uint8_t ignore_missing_src = 0;
	int initial_batch_size = 0;

	while ((ch = getopt(argc, argv, "folis:")) != -1) {
		switch (ch) {
		case 'f':
		case 'o':
			oflag = 1;
			break;
		case 'l':
			lflag = 1;
			break;
		case 'i':
			ignore_missing_src = 1;
			break;
		case 's':
			initial_batch_size = std::stoi(optarg);
			break;
		}
	}
	argc -= optind;
	argv += optind;
	if (argc < 2) {
		snapshot_usage();
		return 1;
	}
	return snapshot(argv[argc - 1], argv, argc - 1, oflag, lflag, ignore_missing_src, initial_batch_size);
}
