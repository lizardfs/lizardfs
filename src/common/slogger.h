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
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/syslog_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

typedef std::shared_ptr<spdlog::logger> LoggerPtr;

namespace lzfs {
namespace log_level {
enum LogLevel {
	trace = spdlog::level::trace,
	debug = spdlog::level::debug,
	info = spdlog::level::info,
	warn = spdlog::level::warn,
	err = spdlog::level::err,
	critical = spdlog::level::critical,
	off = spdlog::level::off
};
} // namespace level

template<typename FormatType, typename... Args>
void log(log_level::LogLevel log_level, const FormatType &format, Args&&... args) {
	//NOTICE(sarna): Workaround for old GCC, which has issues with args... inside lambdas
	small_vector<LoggerPtr, 8> loggers;
	spdlog::apply_all([&loggers](LoggerPtr l) {
		loggers.push_back(l);
	});
	for (LoggerPtr &logger : loggers) {
		logger->log((spdlog::level::level_enum)log_level, format, std::forward<Args>(args)...);
	}
}

template<typename FormatType, typename... Args>
void log_trace(const FormatType &format, Args&&... args) {
	log(log_level::trace, format, std::forward<Args>(args)...);
}

template<typename FormatType, typename... Args>
void log_debug(const FormatType &format, Args&&... args) {
	log(log_level::debug, format, std::forward<Args>(args)...);
}

template<typename FormatType, typename... Args>
void log_info(const FormatType &format, Args&&... args) {
	log(log_level::info, format, std::forward<Args>(args)...);
}

template<typename FormatType, typename... Args>
void log_warn(const FormatType &format, Args&&... args) {
	log(log_level::warn, format, std::forward<Args>(args)...);
}

template<typename FormatType, typename... Args>
void log_err(const FormatType &format, Args&&... args) {
	log(log_level::err, format, std::forward<Args>(args)...);
}

template<typename FormatType, typename... Args>
void log_critical(const FormatType &format, Args&&... args) {
	log(log_level::critical, format, std::forward<Args>(args)...);
}

bool add_log_file(const char *path, log_level::LogLevel level, int max_file_size, int max_file_count);
void set_log_flush_on(log_level::LogLevel level);
void drop_all_logs();
bool add_log_syslog();
bool add_log_stderr(log_level::LogLevel level);

} // namespace lzfs

// NOTICE(sarna) Old interface, don't use unless extern-C is needed
extern "C" {

/// Adds custom logging file
bool lzfs_add_log_file(const char *path, int priority, int max_file_size, int max_file_count);

/// Sets which level triggers immediate log flush (default: CRITICAL)
void lzfs_set_log_flush_on(int priority);

/// Removes all log files
void lzfs_drop_all_logs();

bool lzfs_add_log_syslog();

bool lzfs_add_log_stderr(int priority);

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
