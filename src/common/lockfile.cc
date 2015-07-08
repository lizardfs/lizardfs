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
#include "common/lockfile.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>

#include "common/cwrap.h"
#include "common/massert.h"
#include "common/exceptions.h"

Lockfile::Lockfile(const std::string& name)
		: name_(name) {
}

Lockfile::~Lockfile() {
}

void Lockfile::lock(StaleLock staleLock) {
	sassert(!isLocked()); // TODO what to do instead?
	bool existed = fs::exists(name_);
	if (existed && (staleLock == StaleLock::kReject)) {
		throw LockfileException("stale lockfile exists", LockfileException::Reason::kStaleLock);
	}
	fd_.reset(::open(name_.c_str(), O_CREAT | O_RDWR | (existed ? 0 : O_EXCL), 0644));
	if (!fd_.isOpened()) {
		throw FilesystemException("Cannot open " + name_ + ": " + strerr(errno));
	}

	struct flock fl;
	fl.l_start = 0;
	fl.l_len = 0;
	fl.l_pid = 0;
	fl.l_type = F_WRLCK;
	fl.l_whence = SEEK_SET;
	int ret = fcntl(fd_.get(), F_SETLK, &fl);
	int err = errno; // save errno, we are going to call close()!
	if (ret == 0) {
		return;
	} else {
		fd_.close(); // Lockfile::isLocked <=> fd_.isOpened()
	}
	if (err == EACCES || err == EAGAIN) {
		throw LockfileException(name_ + " is already locked!", LockfileException::Reason::kAlreadyLocked);
	} else {
		throw FilesystemException("Locking: " + name_ + ": " + strerr(err));
	}
}

void Lockfile::unlock() {
	try {
		fd_.close();
		fs::remove(name_);
	} catch (const FilesystemException& e) {
		throw FilesystemException("Unlocking: " + name_ + ": " + e.what());
	}
}

bool Lockfile::isLocked() const {
	return fd_.isOpened();
}

bool Lockfile::hasMessage() const {
	sassert(fd_.isOpened());
	struct stat st;
	if (::fstat(fd_.get(), &st) != 0) {
		throw FilesystemException("fstat of lockfile " + name_ + " failed: " + strerr(errno));
	}
	return st.st_size > 0;
}

void Lockfile::eraseMessage() {
	sassert(fd_.isOpened());
	if (::ftruncate(fd_.get(), 0) != 0) {
		throw FilesystemException("Truncation of lockfile " + name_ + " failed: " + strerr(errno));
	}
}

void Lockfile::writeMessage(std::string const& message) {
	sassert(fd_.isOpened());
	eraseMessage();
	if (::write(fd_.get(), message.data(), message.length()) != static_cast<ssize_t>(message.length())) {
		throw FilesystemException("Writing to lockfile " + name_ + " failed: " + strerr(errno));
	}
}

