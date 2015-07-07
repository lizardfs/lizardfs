/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
