/*
 * TracePrinter.h
 *
 *  Created on: 04-07-2013
 *      Author: Marcin Sulikowski
 */

#pragma once

#ifdef ENABLE_TRACES
#include "config.h"

#include <pthread.h>
#include <sys/time.h>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <boost/format.hpp>

class ThreadPrinter {
private:
	static unsigned& indent(pthread_t threadId) {
		static std::map<pthread_t, unsigned> indents_;
		return indents_[threadId];
	}

	static std::string& colour(pthread_t threadId) {
		static std::map<pthread_t, std::string> colours_;
		return colours_[threadId];
	}

	static const std::string& nextColour() {
		static std::vector<std::string> allColours_ = {
			"\033[37;1m",
			"\033[30;1m",
			"\033[31;1m",
			"\033[32;1m",
			"\033[33;1m",
			"\033[34;1m",
			"\033[35;1m",
			"\033[36;1m",
			"\033[31m",
			"\033[32m",
			"\033[33m",
			"\033[34m",
			"\033[35m",
			"\033[36m",
			"\033[37m",
		};

		static unsigned nextColour_;
		if (nextColour_ == allColours_.size()) {
			nextColour_ = 0;
		}
		return allColours_[nextColour_++];
	}

	static std::mutex& getMutex() {
		static std::mutex mutex;
		return mutex;
	}

public:
	static void printMessage(const std::string& message) {
		pthread_t myId = pthread_self();

		std::unique_lock<std::mutex> lock(getMutex());
		std::string indentStr(indent(myId), ' ');
		std::string& colourStr = colour(myId);
		if (colourStr.empty()) {
			colourStr = nextColour();
		}
		lock.unlock();

		struct timeval tv;
		gettimeofday(&tv, NULL);
		fprintf(stdout, "%s%lu.%06lu Thread %9lx %s%s\033[0m\n", colourStr.c_str(), tv.tv_sec, tv.tv_usec, myId, indentStr.c_str(), message.c_str());
	}

protected:
	static void changeIndent(int delta) {
		std::unique_lock<std::mutex> lock(getMutex());
		indent(pthread_self()) += delta;
	}
};

class TracePrinter : public ThreadPrinter {
private:
	std::string functionName_;
	struct timeval functionCallTimestamp_;
public:
	TracePrinter(const char* functionName, const std::string& arguments)
			: functionName_(functionName)
	{
		printMessage("==> " + functionName_ + arguments);
		changeIndent(4);
		gettimeofday(&functionCallTimestamp_, NULL);
	}

	~TracePrinter() {
		struct timeval endTimestamp;
		gettimeofday(&endTimestamp, NULL);
		size_t usecs = (endTimestamp.tv_sec - functionCallTimestamp_.tv_sec) * 1000000;
		usecs += endTimestamp.tv_usec;
		usecs -= functionCallTimestamp_.tv_usec;
		changeIndent(-4);
		printMessage("<== " + functionName_ + ", usecs=" + std::to_string(usecs));
	}
};


#define TRACETHIS() TracePrinter tracePrinter ## __LINE__(__PRETTY_FUNCTION__, "")
#define TRACETHIS1(a1) TracePrinter tracePrinter ## __LINE__(__PRETTY_FUNCTION__, \
		boost::str(boost::format(" " #a1 "=%1%") % a1))
#define TRACETHIS2(a1, a2) TracePrinter tracePrinter ## __LINE__(__PRETTY_FUNCTION__, \
		boost::str(boost::format(" " #a1 "=%1%, " #a2 "=%2%") % a1 % a2))
#define TRACETHIS3(a1, a2, a3) TracePrinter tracePrinter ## __LINE__(__PRETTY_FUNCTION__, \
		boost::str(boost::format(" " #a1 "=%1%, " #a2 "=%2%, " #a3 "=%3%") % a1 % a2 % a3))
#define TRACETHIS4(a1, a2, a3, a4) TracePrinter tracePrinter ## __LINE__(__PRETTY_FUNCTION__, \
		boost::str(boost::format(" " #a1 "=%1%, " #a2 "=%2%, " #a3 "=%3%, " #a4 "=%4%") % a1 % a2 % a3 % a4))
#define TRACETHIS5(a1, a2, a3, a4, a5) TracePrinter tracePrinter ## __LINE__(__PRETTY_FUNCTION__, \
		boost::str(boost::format(" " #a1 "=%1%, " #a2 "=%2%, " #a3 "=%3%, " #a4 "=%4%, " #a5 "=%5%") % a1 % a2 % a3 % a4 % a5))
#define TRACETHIS6(a1, a2, a3, a4, a5, a6) TracePrinter tracePrinter ## __LINE__(__PRETTY_FUNCTION__, \
		boost::str(boost::format(" " #a1 "=%1%, " #a2 "=%2%, " #a3 "=%3%, " #a4 "=%4%, " #a5 "=%5%, " #a6 "=%6%") % a1 % a2 % a3 % a4 % a5 % a6))
#define PRINTTHIS(a) ThreadPrinter::printMessage(boost::str(boost::format("Line %1%: "#a "=%2%") % __LINE__ % a))
#define PRINTTHISMSG(a) ThreadPrinter::printMessage(boost::str(boost::format("Line %1%: %2%") % __LINE__ % a))
#define MARKTHIS() ThreadPrinter::printMessage(boost::str(boost::format("Line %1%") % __LINE__));
#else
#define TRACETHIS() (void)0
#define TRACETHIS1(a1) (void)0
#define TRACETHIS2(a1, a2) (void)0
#define TRACETHIS3(a1, a2, a3) (void)0
#define TRACETHIS4(a1, a2, a3, a4) (void)0
#define TRACETHIS5(a1, a2, a3, a4, a5) (void)0
#define TRACETHIS6(a1, a2, a3, a4, a5, a6) (void)0
#define PRINTTHIS(a) (void)0
#define PRINTTHISMSG(a) (void)0
#define MARKTHIS() (void)0
#endif /* ENABLE_TRACES */
