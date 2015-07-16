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

#include "common/mfserr.h"
#include "common/debug_log.h"

static bool gPrintf = true;

bool lzfs_is_printf_enabled() {
	return gPrintf;
}

void lzfs_disable_printf() {
	gPrintf = false;
}

void lzfs_enable_printf() {
	gPrintf = true;
}

void lzfs_pretty_prefix(const char* msg) {
	if (lzfs_is_printf_enabled()) {
		fprintf(stderr, "[%s] ", msg);
	}
}

void lzfs_pretty_prefix(int priority) {
	switch (priority) {
		case LOG_ERR:
			lzfs_pretty_prefix("FAIL");
			break;
		case LOG_WARNING:
			lzfs_pretty_prefix("WARN");
			break;
		case LOG_INFO:
		case LOG_NOTICE:
			lzfs_pretty_prefix(" OK ");
			break;
		default:
			lzfs_pretty_prefix("    ");
			break;
	}
}

void lzfs_pretty_prefix_attempt() {
	lzfs_pretty_prefix("....");
}

// Converts first syslog's priority into a human readable string
static std::string syslogLevelToString(int priority) {
	switch (priority) {
	case LOG_EMERG: return "emerg";
	case LOG_ALERT: return "alert";
	case LOG_CRIT: return "crit";
	case LOG_ERR: return "err";
	case LOG_WARNING: return "warning";
	case LOG_NOTICE: return "notice";
	case LOG_INFO: return "info";
	case LOG_DEBUG: return "debug";
	default: return "???";
	}
}

static void lzfs_vsyslog(bool silent, int priority, const char* format, va_list ap) {
	if (!silent && lzfs_is_printf_enabled()) {
		va_list ap2;
		va_copy(ap2, ap);
		vfprintf(stderr, format, ap2);
		va_end(ap2);
		fputc('\n', stderr);
	}
#ifndef _WIN32
	{
		va_list ap2;
		va_copy(ap2, ap);
		vsyslog(priority, format, ap2);
		va_end(ap2);
	}
#endif
	{
		std::string tag = "syslog." + syslogLevelToString(priority);
		va_list ap2;
		va_copy(ap2, ap);
		DEBUG_LOGFV(tag, format, ap2);
		va_end(ap2);
	}
}

void lzfs_pretty_syslog(int priority, const char* format, ...) {
	lzfs_pretty_prefix(priority);
	va_list ap;
	va_start(ap, format);
	lzfs_vsyslog(false, priority, format, ap);
	va_end(ap);
}

void lzfs_pretty_syslog_attempt(int priority, const char* format, ...) {
	lzfs_pretty_prefix_attempt();
	va_list ap;
	va_start(ap, format);
	lzfs_vsyslog(false, priority, format, ap);
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
}

void lzfs_silent_syslog(int priority, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	lzfs_vsyslog(true, priority, format, ap);
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
}
