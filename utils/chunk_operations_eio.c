#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

typedef ssize_t (*pread_t)(int, void *, size_t, off_t);
typedef ssize_t (*pwrite_t)(int, const void *, size_t, off_t);
typedef int (*close_t)(int);
typedef int (*fsync_t)(int);

#define FILENAME_BUFSIZE 1024
#define COMMAND_BUFSIZE FILENAME_BUFSIZE

// only files with this substring in their name ale influenced by this library
#define FILENAME_TRIGGER "/chunk_"

// offset in a file used in some scenarios
#define FAR_OFFSET_THRESHOLD 102400

// for files which match FILENAME_TRIGGER, this library will cause the following functions to fail:
// * pread always fails with EIO if file name contains "pread_EIO"
// * pwrite always fails with EIO if file name contains "pwrite_EIO"
// * close always fails with EIO if file name contains "close_EIO"
// * fsync always fails with EIO if file name contains "fsync_EIO"
// * pread fails with EIO if offset>FAR_OFFSET_THRESHOLD and file name contains "pread_far_EIO"
// * pwrite fails with EIO if offset>FAR_OFFSET_THRESHOLD and file name contains "pwrite_far_EIO"

// returns -1 on failure and sets errno (via readlink call)
ssize_t read_filename(int fd, char *buf, int bufsize) {
	char fdpath[COMMAND_BUFSIZE] = {0};

	sprintf(fdpath, "/proc/self/fd/%d", fd);
	memset(buf, 0, bufsize);
	return readlink(fdpath, buf, bufsize);
}

static int err_on_operation(int fd, const char* opname, size_t offset) {
	char filename[FILENAME_BUFSIZE] = {0};
	char always_eio_trigger[COMMAND_BUFSIZE] = {0};
	char far_eio_trigger[COMMAND_BUFSIZE] = {0};

	ssize_t result = read_filename(fd, filename, FILENAME_BUFSIZE);
	if (result == -1) {
		// cannot read filename, so we assume this file doesn't satisfy the EIO pattern
		return 0;
	}
	if (!strstr(filename, FILENAME_TRIGGER)) {
		return 0;
	}

	// prepare substrings of the filename which trigger errors in various scenarios
	sprintf(always_eio_trigger, "%s_EIO", opname);
	sprintf(far_eio_trigger, "%s_far_EIO", opname);
	if (strstr(filename, always_eio_trigger)) {
		return EIO;
	} else if (strstr(filename, far_eio_trigger) && offset > FAR_OFFSET_THRESHOLD) {
		return EIO;
	} else {
		return 0;
	}
}

// define functions overridden by this library

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
	int err;
	static pread_t _pread = NULL;

	err = err_on_operation(fd, "pread", offset);
	if (err) {
		errno = err;
		return -1;
	}
	if (!_pread) {
		_pread = (pread_t)dlsym(RTLD_NEXT, "pread");
	}
	return _pread(fd, buf, count, offset);
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
	int err;
	static pwrite_t _pwrite = NULL;

	err = err_on_operation(fd, "pwrite", offset);
	if (err) {
		errno = err;
		return -1;
	}
	if (!_pwrite) {
		_pwrite = (pwrite_t)dlsym(RTLD_NEXT, "pwrite");
	}
	return _pwrite(fd, buf, count, offset);
}

int close(int fd) {
	int err;
	static close_t _close = NULL;

	err = err_on_operation(fd, "close", 0);
	if (err) {
		errno = err;
		return -1;
	}
	if (!_close) {
		_close = (close_t)dlsym(RTLD_NEXT, "close");
	}
	return _close(fd);
}

int fsync(int fd) {
	int err;
	static fsync_t _fsync = NULL;

	err = err_on_operation(fd, "fsync", 0);
	if (err) {
		errno = err;
		return -1;
	}
	if (!_fsync) {
		_fsync = (fsync_t)dlsym(RTLD_NEXT, "fsync");
	}
	return _fsync(fd);
}
