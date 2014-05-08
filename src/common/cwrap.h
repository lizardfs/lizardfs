#pragma once

#include "config.h"

#include <dirent.h>
#include <sys/types.h>
#include <cstdio>
#include <memory>

struct CFileCloser {
	void operator()(FILE*) const;
};

struct CDirCloser {
	void operator()(DIR*) const;
};

typedef std::unique_ptr<FILE, CFileCloser> cstream_t;
typedef std::unique_ptr<DIR, CDirCloser> cdirectory_t;
