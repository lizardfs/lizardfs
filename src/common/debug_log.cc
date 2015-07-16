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
#include "common/debug_log.h"

#include <cstdarg>
#include <cstring>

#include "common/mfserr.h"
#include "common/slogger.h"

namespace {

DebugLog::Paths getPathsForPrefix(const std::string& configuration, const std::string& prefix) {
	DebugLog::Paths paths;
	if (configuration.empty()) {
		return paths;
	}
	std::string::size_type pos = 0;
	do {
		std::string::size_type end = configuration.find(",", pos);
		std::string entry =
				configuration.substr(pos, end != std::string::npos ? end - pos : end);
		pos = end != std::string::npos ? end + 1 : end;
		std::string::size_type sepPos = entry.find(":");
		if (sepPos == std::string::npos) {
			continue;
		}
		std::string key = entry.substr(0, sepPos);
		if (prefix.find(key) != std::string::npos) {
			paths.push_back(entry.substr(sepPos + 1));
		}
	} while (pos != std::string::npos);
	return paths;
}

} // anonymous namespace

std::mutex DebugLog::configurationMutex_;
std::string DebugLog::configurationString_;

DebugLog::DebugLog(const Paths& paths) : buffer_(new std::stringstream), streams_() {
	for (const std::string& p : paths) {
		Stream stream(new std::ofstream);
		stream->open(p, std::ios::app);
		if (stream->is_open()) {
			streams_.push_back(std::move(stream));
		} else {
			const char* errMsg = strerr(errno);
			lzfs_pretty_syslog(LOG_ERR, "failed to open log file: %s, %s", p.c_str(), errMsg);
			fprintf(stderr, "failed to open log file: %s, %s\n", p.c_str(), errMsg);
		}
	}
}

void DebugLog::setConfiguration(std::string configuration) {
	std::unique_lock<std::mutex> lock(DebugLog::configurationMutex_);
	DebugLog::configurationString_ = std::move(configuration);
}

std::string DebugLog::getConfiguration() {
	std::unique_lock<std::mutex> lock(DebugLog::configurationMutex_);
	return DebugLog::configurationString_;
}

DebugLog debugLog(const std::string& tag, const char* originFile,
		const char* originFunction, int originLine) {
	DebugLog t(getPathsForPrefix(DebugLog::getConfiguration(), tag));
	static const bool logOrigin = ::getenv("LIZARDFS_LOG_ORIGIN") != nullptr;
	if (logOrigin) {
		static const char* relativePath = strstr(__FILE__, "src/common/debug_log.cc");
		static int const pathNameOffset = relativePath ? relativePath - __FILE__ : 0;
		const char* originFileRelative = originFile + pathNameOffset;
		t << "# " << originFileRelative << ":" << originLine << ":" << originFunction << std::endl;
	}
	t << tag << ": ";
	return t;
}

void debugLogf(const std::string& tag, const char* originFile,
		const char* originFunction, int originLine, const char* format, ...) {
	static const int maxMsgSize = 512;
	char buffer[maxMsgSize];
	va_list va;
	va_start(va, format);
	int len = vsnprintf(buffer, maxMsgSize - 1, format, va);
	va_end(va);
	buffer[len] = 0;
	debugLog(tag, originFile, originFunction, originLine) << buffer;
}

void debugLogfv(const std::string& tag, const char* originFile,
		const char* originFunction, int originLine, const char* format, va_list ap) {
	static const int maxMsgSize = 512;
	char buffer[maxMsgSize];
	va_list ap2;

	va_copy(ap2, ap);
	int len = vsnprintf(buffer, maxMsgSize - 1, format, ap2);
	va_end(ap2);
	buffer[len] = 0;
	debugLog(tag, originFile, originFunction, originLine) << buffer;
}
