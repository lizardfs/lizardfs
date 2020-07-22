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
	static long getIntWithUnitOr(const std::string &option, long defaultValue) {
		return doGetIntFromOption(option, defaultValue, parseIntWithUnit);
	}

	static long getIntOr(const std::string &option, long defaultValue) {
		return doGetIntFromOption(option, defaultValue, parseInt);
	}

	static long parseIntWithUnit(const std::string &text) {
		long mult = 1;
		bool textContainsSuffix = true;
		char last = text[text.size() - 1];

		switch (last) {
			case 'b':
			case 'B':
				break;
			case 'k':
			case 'K':
				mult = 1024L;
				break;
			case 'm':
			case 'M':
				mult = 1024L * 1024;
				break;
			case 'g':
			case 'G':
				mult = 1024L * 1024 * 1024;
				break;
			case 't':
			case 'T':
				mult = 1024L * 1024 * 1024 * 1024;
				break;
			default:
				textContainsSuffix = false;
		}

		std::string intString = textContainsSuffix
			? text.substr(0, text.size() - 1)
			: text;
		long intValue = 0;
		try {
			intValue = parseInt(intString);
		} catch (const std::exception &) {
			throw std::invalid_argument("Cannot parse '" + text + "' as an int with unit.");
		}

		return intValue * mult;
	}

	static long parseInt(const std::string &text) {
		/* Safely convert string to long */
		std::stringstream intStream;
		intStream << text;
		long value;
		std::string rest;
		intStream >> value;
		if (!intStream || (intStream >> rest, rest.size() > 0)) {
			throw std::invalid_argument("Cannot parse '" + text + "' as an int.");
		}
		return value;
	}

	static std::string getOptionValue(const std::string &name,
			const std::string &defaultValue = "") {
		const char* optval = getenv(name.c_str());
		if (optval == NULL) {
			return defaultValue;
		} else {
			return optval;
		}
	}

private:
	using StringToLongFunc = long(*)(const std::string&);
	static long doGetIntFromOption(const std::string &option, long defaultValue, StringToLongFunc parseIntFunc) {
		std::string optval = getOptionValue(option);
		if (optval == "") {
			return defaultValue;
		}
		try {
			return parseIntFunc(optval);
		} catch (const std::exception &e) {
			std::cerr << "Wrong option " << option << " value: '"
					<< optval << "' : " << e.what() << std::endl;
			exit(1);
		}
	}
};
