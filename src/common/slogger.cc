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

#include "common/platform.h"
#include "common/slogger.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#include <string>

#include "common/cfg.h"
#include "common/mfserr.h"

static spdlog::level::level_enum log_level_from_syslog(int priority) {
	static const std::array<spdlog::level::level_enum, 8> kSyslogToLevel = {{
		spdlog::level::critical, // emerg
		spdlog::level::critical, // alert
		spdlog::level::critical, // critical
		spdlog::level::err,      // error
		spdlog::level::warn,     // warning
		spdlog::level::info,     // notice
		spdlog::level::info,     // info
		spdlog::level::debug,    // debug
	}};
	return kSyslogToLevel[std::min<int>(priority, kSyslogToLevel.size())];
}

bool lzfs_add_log_file(const char *path, int priority, int max_file_size, int max_file_count) {
	try {
		LoggerPtr logger = spdlog::rotating_logger_mt(path, path, max_file_size, max_file_count);
		logger->set_level(log_level_from_syslog(priority));
		// Format: DATE TIME [LEVEL] [PID:TID] : MESSAGE
		logger->set_pattern("%D %H:%M:%S.%e [%l] [%P:%t] : %v");
		return true;
	} catch (const spdlog::spdlog_ex &e) {
		lzfs_pretty_syslog(LOG_ERR, "Adding %s log file failed: %s", path, e.what());
	}
	return false;
}

void lzfs_set_log_flush_on(int priority) {
	spdlog::apply_all([priority](LoggerPtr l) {l->flush_on(log_level_from_syslog(priority));});
}

void lzfs_drop_all_logs() {
	spdlog::drop_all();
}

bool lzfs_add_log_stderr(int priority) {
	try {
		LoggerPtr logger = spdlog::stderr_color_mt("stderr");
		logger->set_level(log_level_from_syslog(priority));
		return true;
	} catch (const spdlog::spdlog_ex &e) {
		lzfs_pretty_syslog(LOG_ERR, "Adding stderr log failed: %s", e.what());
	}
	return false;
}

bool lzfs_add_log_syslog() {
#ifndef _WIN32
	try {
		spdlog::syslog_logger("syslog");
		return true;
	} catch (const spdlog::spdlog_ex &e) {
		lzfs_pretty_syslog(LOG_ERR, "Adding syslog log failed: %s", e.what());
	}
#endif
	return false;
}

static void lzfs_vsyslog(int priority, const char* format, va_list ap) {
	char buf[1024];
	va_list ap2;
	va_copy(ap2, ap);
	int written = vsnprintf(buf, 1023, format, ap2);
	if (written < 0) {
		return;
	}
	buf[std::min<int>(written, sizeof(buf))] = '\0';
	va_end(ap2);

	spdlog::apply_all([priority, buf](LoggerPtr l) {
		l->log(log_level_from_syslog(priority), buf);
	});
}

void lzfs_pretty_syslog(int priority, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	lzfs_vsyslog(priority, format, ap);
	va_end(ap);
}

void lzfs_pretty_syslog_attempt(int priority, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	lzfs_vsyslog(priority, format, ap);
	va_end(ap);
}

void lzfs_pretty_errlog(int priority, const char* format, ...) {
	int err = errno;
	char buffer[1024];
	va_list ap;
	va_start(ap, format);
	int len = vsnprintf(buffer, 1023, format, ap);
	buffer[len] = 0;
	va_end(ap);
	lzfs_pretty_syslog(priority, "%s: %s", buffer, strerr(err));
	errno = err;
}

void lzfs_silent_syslog(int priority, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	lzfs_vsyslog(priority, format, ap);
	va_end(ap);
}

void lzfs_silent_errlog(int priority, const char* format, ...) {
	int err = errno;
	char buffer[1024];
	va_list ap;
	va_start(ap, format);
	int len = vsnprintf(buffer, 1023, format, ap);
	buffer[len] = 0;
	va_end(ap);
	lzfs_silent_syslog(priority, "%s: %s", buffer, strerr(err));
	errno = err;
}
