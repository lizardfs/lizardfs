#pragma once

#include "common/platform.h"

#include "common/syslog_defs.h"

/// Returns true iff lzfs_*log functions print messages to stderr.
bool lzfs_is_printf_enabled();

/// Enables printing into stderr in lzfs_*log functions.
void lzfs_disable_printf();

/// Disables printing into stderr in lzfs_*log functions.
void lzfs_enable_printf();

/*
 * function names may contain following words:
 *   "pretty" -> write pretty prefix to stderr
 *   "silent" -> do not write anything to stderr
 *   "errlog" -> append strerr(errno) to printed message
 *   "attempt" -> instead of pretty prefix based on priority, write prefix suggesting
 *      that something is starting
 */

void lzfs_pretty_syslog(int priority, const char* format, ...)
		__attribute__ ((__format__ (__printf__, 2, 3)));

void lzfs_pretty_syslog_attempt(int priority, const char* format, ...)
		__attribute__ ((__format__ (__printf__, 2, 3)));

void lzfs_pretty_errlog(int priority, const char* format, ...)
		__attribute__ ((__format__ (__printf__, 2, 3)));

void lzfs_silent_syslog(int priority, const char* format, ...)
		__attribute__ ((__format__ (__printf__, 2, 3)));

void lzfs_silent_errlog(int priority, const char* format, ...)
		__attribute__ ((__format__ (__printf__, 2, 3)));
