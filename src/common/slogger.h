#pragma once

#include "common/platform.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <syslog.h>

#include "common/lfserr.h"
#include "common/debug_log.h"

#define lfs_syslog(priority,msg) do { \
	syslog((priority),"%s",(msg)); \
	fprintf(stderr,"%s\n",(msg)); \
	DEBUG_LOG("SYS" #priority) << msg; \
} while (false)

#define lfs_arg_syslog(priority,format, ...) do { \
	syslog((priority),(format), __VA_ARGS__); \
	fprintf(stderr,format "\n", __VA_ARGS__); \
	DEBUG_LOGF("SYS" #priority, format, __VA_ARGS__); \
} while (false)

#define lfs_errlog(priority,msg) do { \
	const char *_lfs_errstring = strerr(errno); \
	syslog((priority),"%s: %s", (msg) , _lfs_errstring); \
	fprintf(stderr,"%s: %s\n", (msg), _lfs_errstring); \
	DEBUG_LOG("SYS" #priority) << msg << " " << _lfs_errstring; \
} while (false)

#define lfs_arg_errlog(priority,format, ...) do { \
	const char *_lfs_errstring = strerr(errno); \
	syslog((priority),format ": %s", __VA_ARGS__ , _lfs_errstring); \
	fprintf(stderr,format ": %s\n", __VA_ARGS__ , _lfs_errstring); \
	DEBUG_LOGF("SYS" #priority, format ": %s", __VA_ARGS__, _lfs_errstring); \
} while (false)

#define lfs_errlog_silent(priority,msg) do { \
	syslog((priority),"%s: %s", msg, strerr(errno)); \
	DEBUG_LOG("SYS" #priority) << msg; \
} while (false)

#define lfs_arg_errlog_silent(priority,format, ...) do { \
	syslog((priority),format ": %s", __VA_ARGS__, strerr(errno)); \
	DEBUG_LOGF("SYS" #priority, format ": %s", __VA_ARGS__, strerr(errno)); \
} while (false)

