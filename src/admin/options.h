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

#pragma once

#include "common/platform.h"

#include <cassert>
#include <map>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/massert.h"

class Options {
public:
	LIZARDFS_CREATE_EXCEPTION_CLASS(ParseError, Exception);

	Options(const std::vector<std::string> &expectedArgs, const std::vector<std::string> &argv);

	const std::vector<std::string>& arguments() const {
		return arguments_;
	}

	const std::string& argument(uint32_t pos) const {
		return arguments_[pos];
	}

	bool isSet(const std::string &name) const {
		assert(isOptionExpected(name));
		return options_.at(name);
	}

	template<typename T>
	T getValue(const std::string &name, const T &def = T()) const {
		if (!isSet(name)) {
			return def;
		} else {
			return convert<T>(valued_options_.at(name));
		}
	}

	bool isOptionExpected(const std::string &name) const {
		return options_.count(name) > 0;
	}

	bool isOptionValued(const std::string &name) const {
		return valued_options_.count(name) > 0;
	}

private:
	template<typename T>
	T convert(const std::string &value) const;

	void parseOption(const std::string &arg, bool &expecting_value, std::string &valued_option);

	std::map<std::string, bool> options_;
	std::map<std::string, std::string> valued_options_;
	std::vector<std::string> arguments_;
};

template<>
inline std::string Options::convert(const std::string &str) const {
	return str;
}

template<>
inline int Options::convert(const std::string &str) const {
	return std::stoi(str);
}

template<>
inline long Options::convert(const std::string &str) const {
	return std::stol(str);
}

template<>
inline unsigned long Options::convert(const std::string &str) const {
	return std::stoul(str);
}

template<>
inline long long Options::convert(const std::string &str) const {
	return std::stoll(str);
}

template<>
inline unsigned long long Options::convert(const std::string &str) const {
	return std::stoull(str);
}

template<>
inline float Options::convert(const std::string &str) const {
	return std::stof(str);
}

template<>
inline double Options::convert(const std::string &str) const {
	return std::stod(str);
}

template<>
inline long double Options::convert(const std::string &str) const {
	return std::stold(str);
}
