#include "common/platform.h"
#include "common/lockfile.h"

#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
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

