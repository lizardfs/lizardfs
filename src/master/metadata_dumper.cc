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
#include "master/metadata_dumper.h"

#include <string.h>

#include "common/massert.h"
#include "common/metadata.h"
#include "master/filesystem.h"
#include "master/personality.h"

static bool createPipe(int pipefds[2]) {
	if (pipe(pipefds) != 0) {
		lzfs_pretty_errlog(LOG_ERR, "pipe failed");
		return false;
	}
	return true;
}

MetadataDumper::MetadataDumper(
		const std::string& metadataFilename,
		const std::string& metadataTmpFilename)
		: useMetarestore_(false),
		  dumpingSucceeded_(true),
		  dumpingProcessFd_(-1),
		  dumpingProcessPollFdsPos_(-1),
		  dumpingProcessOutputEmpty_(true),
		  metadataFilename_(metadataFilename),
		  metadataTmpFilename_(metadataTmpFilename) {
}

bool MetadataDumper::dumpSucceeded() const {
	return dumpingSucceeded_;
}

bool MetadataDumper::inProgress() const {
	return dumpingProcessFd_ != -1;
}

bool MetadataDumper::useMetarestore() const {
	return useMetarestore_;
}


void MetadataDumper::setMetarestorePath(const std::string& path) {
	metarestorePath_ = path;
}

void MetadataDumper::setUseMetarestore(bool useMetarestore) {
	useMetarestore_ = useMetarestore;
}

/*
 * Dumping flow:
 * Master creates a child and waits for "OK" or "ERR" message, to see if the dumping  was
 * successful and if the metadata checksums (the one passed from master and the one calculated)
 * match. The child is considered dead when poll returns a 0-sized read or an error on pipe.
 *
 * Dump begins in fs_storeall(). There are 3 cases here.
 * `dumpType` is the argument for storeall.
 * 1) dumpType == kForegroundDump: foreground dump.
 *    Master calls execMetarestore(), which returns false (no fork).
 * 2) dumpType == kBackgroundDump && (!metarestoreSucceeded_ || !useMetarestore_): background
 *    dump when last metarestore failed or when we don't want to use metarestore for dumping
 *    at all.
 *    Master tries to fork and its child, (without execing) dumps metadata.
 *    If everything went well, the child prints "OK" (mocking mfsmetarestore's behaviour),
 *    so that master tries to run metarestore the next time (or he won't - useMetarestore_
 *    isn't changed).
 *    execMetarestore() modifies dumpType to kForegroundDump for the child.
 * 3) dumpType == kBackgroundDump && metarestoreSucceeded_ && useMetarestore_: background dump
 *    when we want to use metarestore and it didn't fail last time.
 *    Master forks and executes mfsmetarestore, which checks checksums and prints "OK" or "ERR".
 *    In case of a syscall error, last metarestore is assumed to have failed
 *    (metarestoreSucceeded_ = false), so that the master dumps its metadata itself.
 *    execMetarestore() modifies dumpType to kForegroundDump for the child.
 */

bool MetadataDumper::start(MetadataDumper::DumpType& dumpType, uint64_t checksum) {
	if (dumpType == kForegroundDump) {
		return false;
	}

	int pipeFd[2] = {-1, -1}; // invalid fds
	dumpingProcessFd_ = -1;
	/*
	 * Changelog files were rotated before entering this function.
	 * Current changelog is now kChangelogFilename + ".1".
	 */
	std::string changelogFilename = kChangelogFilename;
	changelogFilename += ".1";
	if (useMetarestore_ && dumpingSucceeded_ && (access(changelogFilename.c_str(), F_OK) == -1)) {
		if (errno == ENOENT || errno == EACCES) {
			lzfs_pretty_syslog(LOG_ERR, "no current changelog, dump by master");
		} else {
			lzfs_pretty_errlog(LOG_ERR, "access error, dump by master");
		}
		dumpingSucceeded_ = false;
	}

	// can't communicate with child? foreground dump
	if (!createPipe(pipeFd)) {
		lzfs_pretty_syslog(LOG_ERR, "couldn't communicate with child, foreground dump");
		dumpType = kForegroundDump;
		dumpingSucceeded_ = false;
		return false;
	}

	// the child process tells the parent process "OK" or "ERR", until then parent assumes "ERR"
	switch (fork()) {
		case -1:
			// on fork error store metadata in foreground
			lzfs_pretty_errlog(LOG_ERR, "fork failed");
			dumpType = kForegroundDump;
			close(pipeFd[0]); // ignore close errors
			close(pipeFd[1]);
			return false;
		case 0:
			close(pipeFd[0]); // ignore close error
			if (dup2(pipeFd[1], STDOUT_FILENO)  == -1) {
				// can't give the response
				lzfs_pretty_errlog(LOG_ERR, "dup2 failed, dump by master");
				dumpingSucceeded_ = false;
			}
			if (useMetarestore_ && dumpingSucceeded_) {
				// exec mfsmetarestore
				std::string checksumStringified = std::to_string(checksum);
				std::string storedMetaCopies = std::to_string(gStoredPreviousBackMetaCopies);
				char* metarestoreArgs[] = {
					const_cast<char*>(metarestorePath_.c_str()),
					const_cast<char*>("-m"),
					const_cast<char*>(metadataFilename_.c_str()),
					const_cast<char*>("-o"),
					const_cast<char*>(metadataTmpFilename_.c_str()),
					const_cast<char*>("-k"),
					const_cast<char*>(checksumStringified.c_str()),
					const_cast<char*>("-B"),
					const_cast<char*>(storedMetaCopies.c_str()),
					const_cast<char*>("-#"),
					const_cast<char*>(changelogFilename.c_str()),
					NULL};
				// the default value of the commandline nice
				if (nice(10) == -1) {
					lzfs_pretty_errlog(LOG_WARNING, "dumping metadata: nice failed");
				}
				execv(metarestorePath_.c_str(), metarestoreArgs);
				lzfs_pretty_syslog(LOG_WARNING, "exec %s failed: %s", metarestorePath_.c_str(), strerr(errno));
			}
			if (useMetarestore_ && !dumpingSucceeded_) {
				lzfs_pretty_syslog(LOG_NOTICE, "something previously failed, dump by master");
			}
			dumpType = kForegroundDump; // child process stores metadata in its foreground
			return true;
		default:
			dumpingProcessOutputEmpty_ = true;
			dumpingSucceeded_ = false;
			dumpingProcessFd_ = pipeFd[0];
			close(pipeFd[1]);
			return false;
	}
}

// for poll
void MetadataDumper::pollDesc(std::vector<pollfd> &pdesc) {
	if (dumpingProcessFd_ != -1) {
		pdesc.push_back({dumpingProcessFd_,POLLIN,0});
		dumpingProcessPollFdsPos_ = pdesc.size() - 1;
	} else {
		dumpingProcessPollFdsPos_ = -1;
	}
}

void MetadataDumper::pollServe(const std::vector<pollfd> &pdesc) {
	if (dumpingProcessPollFdsPos_ == -1) {
		return;
	}
	if (pdesc[dumpingProcessPollFdsPos_].revents & POLLIN) {
		char buffer[1024];
		int ret = read(dumpingProcessFd_, buffer, sizeof(buffer) - 1);
		if (ret == -1) {
			lzfs_pretty_errlog(LOG_WARNING, "read from the process dumping metadata failed");
			dumpingFinished();
		} else if (ret == 0) {
			dumpingFinished();
		} else {
			buffer[ret] = '\0';
			dumpingProcessOutputEmpty_ = false;
			dumpingSucceeded_ = std::string(buffer) == "OK\n";
			if (!dumpingSucceeded_) {
				lzfs_pretty_syslog(LOG_WARNING, "metadata dumping failed: expected 'OK', received '%s'",
						buffer);
			}
		}
	}
	if (pdesc[dumpingProcessPollFdsPos_].revents & POLLERR
			|| pdesc[dumpingProcessPollFdsPos_].revents & POLLHUP
			|| pdesc[dumpingProcessPollFdsPos_].revents & POLLNVAL) {
		dumpingFinished();
	}
}

void MetadataDumper::dumpingFinished() {
	if (close(dumpingProcessFd_) == -1) {
		lzfs_pretty_errlog(LOG_ERR, "pipe close failed");
	}
	dumpingProcessFd_ = -1;
	dumpingProcessPollFdsPos_ = -1;
	if (dumpingProcessOutputEmpty_) {
		lzfs_pretty_syslog(LOG_WARNING, "the dumping process finished without producing output");
	}
}

void MetadataDumper::waitUntilFinished(SteadyDuration timeout) {
	// pollDesc uses at most one pollfd
	std::vector<pollfd> pfd;
	Timeout stopwatch(timeout);
	while (inProgress() && !stopwatch.expired()) {
		pfd.clear();
		pollDesc(pfd);
		sassert(pfd.size() <= 1);
		// on 0 `poll' returns immediately and that's fine
		if (poll(pfd.data(), pfd.size(), stopwatch.remaining_ms()) == -1) {
			lzfs_pretty_errlog(LOG_ERR, "poll error during waiting for dumping to finish");
			break;
		}
		pollServe(pfd);
	}
	if (inProgress()) {
		lzfs_pretty_syslog(LOG_ERR, "dumping didn't finish in specified timeout: %.2f s",
				std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(
					timeout).count());
		// mark finish anyway
		dumpingFinished();
	}
}

void MetadataDumper::waitUntilFinished() {
	// let's say a year is enough
	waitUntilFinished(std::chrono::hours(24 * 365));
}

