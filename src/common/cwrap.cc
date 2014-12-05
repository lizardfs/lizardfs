#include "common/platform.h"
#include "common/cwrap.h"

#include <libgen.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>

#include "common/exceptions.h"
#include "common/massert.h"

FileDescriptor::FileDescriptor()
		: fd_(-1) {
}

FileDescriptor::FileDescriptor(int fd)
		: fd_(fd) {
}

FileDescriptor::~FileDescriptor() {
	if (fd_ >= 0) {
		close();
	}
}

int FileDescriptor::get() const {
	return fd_;
}

void FileDescriptor::reset(int fd) {
	if (fd_ >= 0) {
		close();
	}
	fd_ = fd;
}

void FileDescriptor::close() {
	massert(fd_ >= 0, "file descriptor is not opened");
	::close(fd_);
	fd_ = -1;
}

bool FileDescriptor::isOpened() const {
	return fd_ >= 0;
}

void CFileCloser::operator()(FILE* file_) const {
	::std::fclose(file_);
}

void CDirCloser::operator()(DIR* dir) const {
	closedir(dir);
}

std::string errorString(int errNo) {
	return strerror(errNo);
}

namespace fs {

bool exists(const std::string& path) {
	int err = access(path.c_str(), F_OK);
	if (err != 0 && errno != ENOENT) {
		throw FilesystemException(errorString(errno));
	}
	return err == 0;
}

void rename(const std::string& currentPath, const std::string& newPath) {
	int err = ::rename(currentPath.c_str(), newPath.c_str());
	if (err != 0) {
		throw FilesystemException(errorString(errno));
	}
}

void remove(const std::string& path) {
	int err = ::remove(path.c_str());
	if (err != 0) {
		throw FilesystemException(errorString(errno));
	}
}

typedef std::unique_ptr<char[], decltype(&free)> cstr;

std::string dirname(const std::string& path) {
	cstr copy = cstr(strdup(path.c_str()), &free);
	std::string dirName = ::dirname(copy.get());
	return dirName;
}

std::string getCurrentWorkingDirectory() {
	cstr value = cstr(getcwd(nullptr, 0), &free);
	if (value == nullptr) {
		throw FilesystemException("getcwd failed");
	}
	return std::string(value.get());
}

std::string getCurrentWorkingDirectoryNoThrow() {
	std::string currentPath;
	try {
		currentPath = fs::getCurrentWorkingDirectory();
	} catch (const FilesystemException& ex) {
		syslog(LOG_WARNING, "unable to get current working directory %s", ex.what());
		currentPath = "???";
	}
	return currentPath;
}

}
