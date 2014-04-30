#pragma once

#include "config.h"

#include <memory>

class FileDescriptor {
public:
	FileDescriptor();
	FileDescriptor(int fd);
	FileDescriptor(const FileDescriptor&) = delete;
	FileDescriptor& operator=(const FileDescriptor&) = delete;
	virtual ~FileDescriptor();
	int get() const;
	void reset(int fd);
	void close();
	bool isOpened() const;

private:
	int fd_;
};

struct CFileCloser {
	void operator()(FILE*) const;
};

typedef std::unique_ptr<FILE, CFileCloser> cstream_t;

