#include "human_readable_format.h"

#include <cmath>
#include <iomanip>
#include <sstream>
#include <string>

#define BASE_SI 1000
#define BASE_IEC 1024

static std::string convertToHumanReadableFormat(const uint64_t inputNumber, const uint16_t base){
	std::stringstream ss;

	ss << std::fixed;
	if (inputNumber < base) {
		ss << inputNumber;
	} else {
		uint16_t exp = static_cast<uint16_t>(log(inputNumber) / log(base));
		double number = inputNumber / pow(base, exp);

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
