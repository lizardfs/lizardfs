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

#ifndef _WIN32
#define SPDLOG_ENABLE_SYSLOG
#endif
#include "common/small_vector.h"
#include "spdlog/spdlog.h"

typedef std::shared_ptr<spdlog::logger> LoggerPtr;

extern "C" {

/// Adds custom logging file
bool lzfs_add_log_file(const char *path, int priority, int max_file_size, int max_file_count);

/// Sets which level triggers immediate log flush (default: CRITICAL)
void lzfs_set_log_flush_on(int priority);

/// Removes all log files
void lzfs_drop_all_logs();

bool lzfs_add_log_syslog();

bool lzfs_add_log_stderr();

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

template<typename FormatType, typename... Args>
void lzfs_log(spdlog::level::level_enum level, const FormatType &format, Args&&... args) {
	//NOTICE(sarna): Workaround for old GCC, which has issues with args... inside lambdas
	small_vector<LoggerPtr, 8> loggers;
	spdlog::apply_all([&loggers](LoggerPtr l) {
		loggers.push_back(l);
	});
	for (LoggerPtr &logger : loggers) {
		logger->log(level, format, std::forward<Args>(args)...);
	}
}
