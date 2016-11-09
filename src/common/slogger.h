/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include "common/syslog_defs.h"

extern "C" {
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
} // extern "C"
