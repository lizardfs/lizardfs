#pragma once

#include "common/platform.h"

#include <signal.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <string>

class Configuration {
protected:

static long getIntOr(const std::string& option, long defaultValue) {
	std::string optval = getOptionValue(option);
	if (optval == "") {
		return defaultValue;
	}
	long mult = 1;
	char last = optval[optval.size() - 1];
	if (last == 'k' || last == 'K') {
		mult = 1024L;
	} else if (last == 'm' || last == 'M') {
		mult = 1024L * 1024;
	} else if (last == 'g' || last == 'G') {
		mult = 1024L * 1024 * 1024;
	} else if (last == 't' || last == 'T') {
		mult = 1024L * 1024 * 1024 * 1024;
	}
	if (mult > 1) {
		// Remove the last character if it was K/M/G/T
		optval.erase(optval.size() - 1);
	}
	/* Safely convert string to long */
	std::stringstream intStream;
	intStream << optval;
	long value;
	intStream >> value;
	if (!intStream) {
		std::cerr << "Wrong option " << option << " value: '"
				<< getOptionValue(option) << "'" << std::endl;
		exit(1);
	}
	return value * mult;
}

static std::string getOptionValue(const std::string& name,
		const std::string& defaultValue = "") {
	const char* optval = getenv(name.c_str());
	if (optval == NULL) {
		return defaultValue;
	} else {
		return optval;
	}
}
};
