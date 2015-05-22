#include "common/platform.h"
#include "common/human_readable_format.h"

#include <cmath>
#include <iomanip>
#include <sstream>
#include <string>

#include "common/massert.h"

#define BASE_SI 1000
#define BASE_IEC 1024

static std::string convertToHumanReadableFormat(const uint64_t inputNumber, const uint16_t base){
	std::stringstream ss;

	ss << std::fixed;
	if (inputNumber < base) {
		ss << inputNumber;
	} else {
		uint16_t exp = static_cast<uint16_t>(std::log(inputNumber) / std::log(base));
		double number = inputNumber / std::pow(base, exp);

		if (number > base - 1) { // e.g. 1023.9MiB is almost like 1024MiB, so we convert it to 1.0GiB
			number /= base;
			++exp;
		}

		// when number has one digit increase precision e.g. 2300 bytes is 2.3k instead of 2k
		ss << std::setprecision(number < 10 ? 1 : 0);

		if (base == BASE_SI) {
			ss << number << "kMGTPE"[exp - 1];
		} else {
			ss << number << "KMGTPE"[exp - 1] << "i";
		}
	}

	return ss.str();
}

std::string convertToSi(const uint64_t number) {
	return convertToHumanReadableFormat(number, BASE_SI);
}

std::string convertToIec(const uint64_t number) {
	return convertToHumanReadableFormat(number, BASE_IEC);
}

std::string ipToString(uint32_t ip) {
	std::stringstream ss;
	for (int i = 24; i >= 0; i -= 8) {
		ss << ((ip >> i) & 0xff) << (i > 0 ? "." : "");
	}
	return ss.str();
}

std::string timeToString(time_t time) {
	char timeBuffer[32];
	strftime(timeBuffer, 32, "%Y-%m-%d %H:%M:%S", localtime(&time));
	return timeBuffer;
}

std::string bpsToString(uint64_t bytes, uint64_t usec) {
	sassert(usec > 0);
	std::stringstream ss;
	ss << convertToIec((bytes * 1000000.0) / usec) << "B/s";
	return ss.str();
}
