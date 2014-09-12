#include "common/platform.h"
#include "common/debug_log.h"

#include <cstdarg>
#include <cstring>

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
			syslog(LOG_ERR, "failed to open log file: %s, %s", p.c_str(), errMsg);
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
