#pragma once

#include "common/platform.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <syslog.h>

#include "common/mfserr.h"
#include "common/debug_log.h"

#define mfs_syslog(priority,msg) do { \
	syslog((priority),"%s",(msg)); \
	fprintf(stderr,"%s\n",(msg)); \
	DEBUG_LOG("SYS" #priority) << msg; \
} while (false)

#define mfs_arg_syslog(priority,format, ...) do { \
	syslog((priority),(format), __VA_ARGS__); \
	fprintf(stderr,format "\n", __VA_ARGS__); \
	DEBUG_LOGF("SYS" #priority, format, __VA_ARGS__); \
} while (false)

#define mfs_errlog(priority,msg) do { \
	const char *_mfs_errstring = strerr(errno); \
	syslog((priority),"%s: %s", (msg) , _mfs_errstring); \
	fprintf(stderr,"%s: %s\n", (msg), _mfs_errstring); \
	DEBUG_LOG("SYS" #priority) << msg << " " << _mfs_errstring; \
} while (false)

#define mfs_arg_errlog(priority,format, ...) do { \
	const char *_mfs_errstring = strerr(errno); \
	syslog((priority),format ": %s", __VA_ARGS__ , _mfs_errstring); \
	fprintf(stderr,format ": %s\n", __VA_ARGS__ , _mfs_errstring); \
	DEBUG_LOGF("SYS" #priority, format ": %s", __VA_ARGS__, _mfs_errstring); \
} while (false)

#define mfs_errlog_silent(priority,msg) do { \
	syslog((priority),"%s: %s", msg, strerr(errno)); \
	DEBUG_LOG("SYS" #priority) << msg; \
} while (false)

#define mfs_arg_errlog_silent(priority,format, ...) do { \
	syslog((priority),format ": %s", __VA_ARGS__, strerr(errno)); \
	DEBUG_LOGF("SYS" #priority, format ": %s", __VA_ARGS__, strerr(errno)); \
} while (false)

