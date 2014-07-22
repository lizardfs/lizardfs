#include "common/platform.h"
#include "common/debug_log.h"

#include <cstdarg>
#include <cstring>

#include "common/cfg.h"
#include "common/slogger.h"

namespace {

DebugLog::Paths getPathsForPrefix(const std::string& prefix) {
	std::string magicDebugLog = cfg_get("MAGIC_DEBUG_LOG", std::string());
	if (magicDebugLog.empty()) {
		return DebugLog::Paths();
	}
	std::string::size_type pos = 0;
	DebugLog::Paths paths;
	do {
		std::string::size_type end = magicDebugLog.find(",", pos);
		std::string entry(magicDebugLog.substr(pos, end != std::string::npos ? end - pos : end));
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

}

DebugLog::DebugLog(const Paths& paths)
		: buffer_(new std::stringstream), streams_() {
	for (const std::string& p : paths) {
		Stream stream(new std::ofstream);
		stream->open(p, std::ios::app);
		if (stream->is_open()) {
			streams_.push_back(std::move(stream));
		} else {
			const char* errMsg = strerr(errno);
			syslog(LOG_ERR, "failed to open log file: %s, %s", p.c_str(), errMsg);
			fprintf(stderr, "failed to open log file: %s, %s\n", p.c_str(), errMsg);
		}
	}
}

DebugLog debugLog(const std::string& tag, const char* originFile,
		const char* originFunction, int originLine) {
	DebugLog t(getPathsForPrefix(tag));
	static const bool logOrigin = ::getenv("LIZARDFS_LOG_ORIGIN") != nullptr;
	t << tag << ": ";
	if (logOrigin) {
		static const char* relativePath = strstr(__FILE__, "src/common/debug_log.cc");
		static int const pathNameOhffset = relativePath ? relativePath - __FILE__ : 0;
		t << (originFile + pathNameOhffset) << ":" << originLine << ":" << originFunction << ": ";
	}
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

