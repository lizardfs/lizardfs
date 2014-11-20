#include "common/platform.h"
#include "common/lockfile.h"

#include <unistd.h>
#include <sys/file.h>

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
		FilesystemException("Cannot open " + name_ + ": " + strerr(errno));
	}
	int ret = flock(fd_.get(), LOCK_EX | LOCK_NB);
	if (ret == 0) {
		return;
	} else {
		fd_.close(); // Lockfile::isLocked <=> fd_.isOpened()
	}
	if (errno == EWOULDBLOCK) {
		throw LockfileException(name_ + " is already locked!", LockfileException::Reason::kAlreadyLocked);
	} else {
		throw FilesystemException("Locking: " + name_ + ": " + strerr(errno));
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

