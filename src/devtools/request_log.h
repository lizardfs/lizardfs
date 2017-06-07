/*
   Copyright 2013-2017 Skytechnology sp. z o.o.

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

#ifdef ENABLE_REQUEST_LOG
#include "common/platform.h"

#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include "common/massert.h"
#include "common/slogger.h"
#include "devtools/configuration.h"

class RequestLogConfiguration : Configuration {
public:
	static std::string compressionAlgorithm() {
		return Configuration::getOptionValue("REQUESTS_LOG_COMPRESSION_ALG", "gzip");
	}

	static long flushSleep_s() {
		return Configuration::getIntOr("REQUESTS_LOG_FLUSH_SLEEP_SEC", 1);
	}

	static std::string logPath() {
#ifdef _WIN32
		return Configuration::getOptionValue("REQUESTS_LOG", "C:\\Windows\\Temp\\requests.log");
#else
		return Configuration::getOptionValue("REQUESTS_LOG", "/tmp/requests.log");
#endif
	}

	static long requestsToBeLogged() {
		return Configuration::getIntOr("REQUESTS_TO_BE_LOGGED",
				40*1000*1000 /* This will occupy about 3.5GB of RAM!*/);
	}

	static long requestThreshold_ms() {
		return Configuration::getIntOr("REQUEST_THRESHOLD_MS", 1000);
	}
};

class DummyRTTimer {
public:
	DummyRTTimer() {
		gettimeofday(&startTime, NULL);
	}

	uint64_t getCreationUsectime() const {
		return ((uint64_t)(startTime.tv_sec)) * 1000000 + startTime.tv_usec;
	}

	uint64_t getLifeUsectime() const {
		DummyRTTimer currentTime;
		return currentTime.getCreationUsectime() - getCreationUsectime();
	}

	void reset() {
		*this = DummyRTTimer();
	}

private:
	struct timeval startTime;
};

class Compressor {
private:
	std::ofstream logfile;
	boost::iostreams::filtering_stream<boost::iostreams::output> outputStream;

public:
	Compressor(std::string logFileName, bool forceDisableCompression = false) {
		logfile.open(logFileName, std::ios_base::out | std::ios_base::binary);
		if (!forceDisableCompression) {
			std::string compressionAlgorithm = RequestLogConfiguration::compressionAlgorithm();
			if (compressionAlgorithm == "gzip") {
				outputStream.push(boost::iostreams::gzip_compressor());
			} else if (compressionAlgorithm == "bzip2") {
				outputStream.push(boost::iostreams::bzip2_compressor());
			} else if (compressionAlgorithm != "none") {
				mabort("Unknown request log compression algorithm!");
			}
		}
		outputStream.push(logfile);
	}

	template<class T>
	Compressor& operator<<(const T& t) {
		outputStream << t;
		return *this;
	}

	void flush() {
		logfile.flush();
		outputStream.flush();
	}

	~Compressor() {
		outputStream.reset();
		logfile.close();
	}
};

template<class OutputStream>
class TuplePrinter {
private:
	template<std::size_t> struct int_{};

	template <class Tuple, size_t Position, class Separator>
	static OutputStream& print_tuple_internal(
			OutputStream& outputStream, const Tuple& t, int_<Position>, Separator separator) {
		outputStream << std::get<std::tuple_size<Tuple>::value - Position>(t) << separator;
		return print_tuple_internal(outputStream, t, int_<Position - 1>(), separator);
	}

	template <class Tuple, class Separator>
	static OutputStream& print_tuple_internal(OutputStream& outputStream, const Tuple& t, int_<1>,
			Separator) {
		return outputStream << std::get<std::tuple_size<Tuple>::value - 1>(t) << '\n';
	}

public:
	template<class... Args, class Separator>
	static OutputStream& print_tuple(OutputStream& outputStream, const std::tuple<Args...>& t,
			Separator separator) {
		return print_tuple_internal(outputStream, t, int_<sizeof...(Args)>(), separator);
	}
};

class RequestsLog {
public:
	typedef uint64_t TimeInMicroseconds;
	typedef std::string Operation;
	typedef uint64_t ChunkID;
	typedef uint64_t Offset;
	typedef uint64_t Size;
	typedef uint64_t LongKey;

private:
	typedef TimeInMicroseconds Duration;

	typedef TimeInMicroseconds StartTime;
	typedef std::tuple<StartTime, Operation, ChunkID, Offset, Size, Duration> Request;
	typedef std::vector<Request> Requests;

	typedef std::tuple<Operation, LongKey, LongKey> AverageRequest;
	typedef Duration AvgDuration;
	typedef uint64_t NrOfOccurences;
	typedef Duration MaxDuration;
	typedef std::map<AverageRequest,
			std::tuple<AvgDuration, NrOfOccurences, MaxDuration>> AverageRequestTimes;

	Requests requests;
	AverageRequestTimes selectedRequestsAverages;
	const uint64_t requestsToBeLogged;
	const uint64_t requestsTimeThreshold;
	std::mutex logMutex;
	std::mutex avgLogMutex;
	std::atomic<bool> doTerminate;

	class Flusher {
	private:
		RequestsLog& requestsLog;
		std::atomic<bool>& doTerminate;
		static const uint64_t millisecondsInASecond = 1000000;
		const TimeInMicroseconds sleepTime;
		std::string logFileName;
		Compressor logOutputStream;
		std::string avgLogFileName;
		Compressor avgLogOutputStream;

	public:
		Flusher(RequestsLog& requestsLog_, std::atomic<bool>& doTerminate_) : requestsLog(requestsLog_),
				doTerminate(doTerminate_),
				sleepTime(millisecondsInASecond * RequestLogConfiguration::flushSleep_s()),
				logFileName(RequestLogConfiguration::logPath()),
				logOutputStream(logFileName),
				avgLogFileName(logFileName + ".avg"),
				avgLogOutputStream(avgLogFileName, true)
		{
			std::cout << "REQUESTS_LOG_FLUSH_SLEEP_SEC: " << sleepTime / millisecondsInASecond
					<< std::endl;
			std::cout << "REQUESTS_LOG: " << logFileName << std::endl;
		}

		void operator()() {
			while (!doTerminate) {
				DummyRTTimer timer;

				Requests requests;
				AverageRequestTimes selectedRequestsAvgTimes_;
				requestsLog.swap(requests, selectedRequestsAvgTimes_);

				// Request log
				if (!requests.empty()) {
					std::cout << "Flushing requests log to a file " << logFileName << " ... ";
					try {
						for (auto& tup : requests) {
							TuplePrinter<Compressor>::print_tuple(logOutputStream, tup, '\t');
						}
						std::cout << "done!" << std::endl;
					} catch (...) {
						std::cout << "FAILED!" << std::endl;
						lzfs_pretty_syslog(LOG_WARNING, "Failed to flush the request log");
					}
				}
				logOutputStream.flush();

				// Request average log
				std::cout << "Flushing avg requests log a file " << avgLogFileName << " ... ";
				auto timestamp = std::make_tuple(timer.getCreationUsectime());
				for (auto& it : selectedRequestsAvgTimes_) {
					auto average = std::make_tuple(
							std::get<0>(it.second) / std::max<uint64_t>(1UL, std::get<1>(it.second)));
					TuplePrinter<Compressor>::print_tuple(
							avgLogOutputStream,
							std::tuple_cat(timestamp, std::tuple_cat(it.first,
									std::tuple_cat(it.second, average))),
							'\t');
				}
				avgLogOutputStream.flush();
				std::cout << "done!" << std::endl;

				usleep(std::max(int64_t(0),
						(int64_t)sleepTime - (int64_t)timer.getLifeUsectime()));
			}
		}
	};

	Flusher flusher;
	std::thread flushingThread;

	RequestsLog() :
			requestsToBeLogged(RequestLogConfiguration::requestsToBeLogged()),
			requestsTimeThreshold(RequestLogConfiguration::requestThreshold_ms()),
			doTerminate(false), flusher(*this, doTerminate), flushingThread(std::ref(flusher)) {
		std::cout << "REQUESTS_TO_BE_LOGGED: " << requestsToBeLogged << std::endl;
		// reserve in advance, to save time doing real logging:
		requests.reserve(requestsToBeLogged);
	}

public:
	static RequestsLog& instance() {
		static RequestsLog instance;
		return instance;
	}

	void log(const Operation& operation, ChunkID chunkID, Offset offset, Size size,
			TimeInMicroseconds startTime = DummyRTTimer().getCreationUsectime(),
			TimeInMicroseconds duration = 0) {
		if (duration < requestsTimeThreshold) {
			return;
		}
		auto request = std::make_tuple(startTime, operation, chunkID, offset, size, duration);
		// acquire mutex after creating a tuple, to make critical section shorter
		std::unique_lock<std::mutex> lock(logMutex);
		if (requests.size() < requestsToBeLogged) {
			requests.push_back(request);
			if (requests.size() == requestsToBeLogged) {
				std::cerr << "Requests log full!" << std::endl;
				lzfs_pretty_syslog(LOG_WARNING, "Requests log full");
			}
		}
	}

	void log(const Operation& operation, ChunkID chunkID, Offset offset,
			Size size, const DummyRTTimer& timer) {
		log(operation, chunkID, offset, size, timer.getCreationUsectime(),
				timer.getLifeUsectime());
	}

	void logAvg(const Operation& operation, LongKey key1, LongKey key2,
			TimeInMicroseconds duration) {
		std::unique_lock<std::mutex> lock(avgLogMutex);
		auto& req = selectedRequestsAverages[std::make_tuple(operation, key1, key2)];
		req = std::make_tuple(
				// Update sum
				std::get<0>(req) + duration,
				// Update counter
				std::get<1>(req) + 1,
				// Update max
				std::max<TimeInMicroseconds>(std::get<2>(req), duration));
	}

	void swap(Requests& requests_, AverageRequestTimes& selectedAvgRequests_) {
		{
			// reserve in advance, to save time doing real logging:
			requests_.reserve(requestsToBeLogged);
			// reserve before acquiring the lock, to make critical section shorter:
			std::unique_lock<std::mutex> lock(logMutex);
			std::swap(requests, requests_);
		}
		{
			std::unique_lock<std::mutex> lock(avgLogMutex);
			std::swap(selectedRequestsAverages, selectedAvgRequests_);
		}
	}

	~RequestsLog() {
		doTerminate = true;
		flushingThread.join();
	}
};

class FunctionCallLog {
public:
	typedef RequestsLog::Operation Operation;
	typedef RequestsLog::ChunkID ChunkID;
	typedef RequestsLog::Offset Offset;
	typedef RequestsLog::Size Size;

	FunctionCallLog(const Operation& operation_, ChunkID chunkID_, Offset offset_, Size size_)
		: operation(operation_), chunkID(chunkID_), offset(offset_), size(size_) {
	}

	~FunctionCallLog() {
		RequestsLog::instance().log(operation, chunkID, offset, size, timer);
	}

private:
	Operation operation;
	ChunkID chunkID;
	Offset offset;
	Size size;
	DummyRTTimer timer;
};

class FunctionCallAvgLog {
public:
	typedef RequestsLog::Operation Operation;
	typedef RequestsLog::LongKey Key1;
	typedef RequestsLog::LongKey Key2;

	FunctionCallAvgLog(const Operation& operation_, Key1 key1_ = 0U, Key2 key2_ = 0U)
		: operation(operation_), key1(key1_), key2(key2_) {
	}

	~FunctionCallAvgLog() {
		RequestsLog::instance().logAvg(operation, key1, key2, timer.getLifeUsectime());
	}

private:
	Operation operation;
	Key1 key1;
	Key1 key2;
	DummyRTTimer timer;
};

#define CONCAT1(x,y) x ## y
#define CONCAT2(x,y) CONCAT1(x,y)
#define REQUEST_LOG_UNIQ_NAME1(y1) CONCAT2(LogObject, __LINE__) (y1)
#define REQUEST_LOG_UNIQ_NAME2(y1, y2) CONCAT2(LogObject, __LINE__) (y1, y2)
#define REQUEST_LOG_UNIQ_NAME3(y1, y2, y3) CONCAT2(LogObject, __LINE__) (y1, y2, y3)
#define REQUEST_LOG_UNIQ_NAME4(y1, y2, y3, y4) CONCAT2(LogObject, __LINE__) (y1, y2, y3, y4)


#define LOG_TILL_END_OF_SCOPE(operation, chunkID, offset, size) \
		FunctionCallLog REQUEST_LOG_UNIQ_NAME4(operation, chunkID, offset, size)
#define LOG_REQUEST(operation, chunkID, offset, size, timer) \
		RequestsLog::instance().log(operation, chunkID, offset, size, timer)
#define LOG_REQUEST_RESET_TIMER(operation, chunkID, offset, size, timer) \
		RequestsLog::instance().log(operation, chunkID, offset, size, timer); timer.reset()
#define LOG_POINT_IN_TIME(operation, chunkID, offset, size) \
		RequestsLog::instance().log(operation, chunkID, offset, size)
#define LOG_AVG_TILL_END_OF_SCOPE2(operation, key1, key2) \
		FunctionCallAvgLog REQUEST_LOG_UNIQ_NAME3(operation, key1, key2)
#define LOG_AVG_TILL_END_OF_SCOPE1(operation, key1) \
		FunctionCallAvgLog REQUEST_LOG_UNIQ_NAME2(operation, key1)
#define LOG_AVG_TILL_END_OF_SCOPE0(operation) \
		FunctionCallAvgLog REQUEST_LOG_UNIQ_NAME1(operation)
#define LOG_AVG_TYPE std::unique_ptr<FunctionCallAvgLog>
#define LOG_AVG_START0(obj, operation) obj.reset(new FunctionCallAvgLog(operation))
#define LOG_AVG_START1(obj, operation, key1) obj.reset(new FunctionCallAvgLog(operation, key1))
#define LOG_AVG_STOP(obj) obj.reset()
#else
struct DummyRTTimer {};
#define LOG_TILL_END_OF_SCOPE(operation, chunkID, offset, size) (void)0
#define LOG_REQUEST(operation, chunkID, offset, size, timer) (void)0
#define LOG_REQUEST_RESET_TIMER(operation, chunkID, offset, size, timer) (void)0
#define LOG_POINT_IN_TIME(operation, chunkID, offset, size) (void)0
#define LOG_AVG_TILL_END_OF_SCOPE2(operation, key1, key2) (void)0
#define LOG_AVG_TILL_END_OF_SCOPE1(operation, key1) (void)0
#define LOG_AVG_TILL_END_OF_SCOPE0(operation) (void)0
#define LOG_AVG_TYPE DummyRTTimer
#define LOG_AVG_START0(obj, operation) (void)0
#define LOG_AVG_START1(obj, operation, key1) (void)0
#define LOG_AVG_STOP(obj) (void)0
#endif /* ENABLE_REQUEST_LOG */
