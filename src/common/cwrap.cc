#include "config.h"
#include "common/cwrap.h"

#include <unistd.h>
#include <cstdio>

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
	TEMP_FAILURE_RETRY(::close(fd_));
	fd_ = -1;
}

bool FileDescriptor::isOpened() const {
	return fd_ >= 0;
}

void CFileCloser::operator()(FILE* file_) const {
	::std::fclose(file_);
}

