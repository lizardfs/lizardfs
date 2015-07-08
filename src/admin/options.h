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

#include <map>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/massert.h"

class Options {
public:
	LIZARDFS_CREATE_EXCEPTION_CLASS(ParseError, Exception);

	Options(const std::vector<std::string>& expectedArgs, const std::vector<std::string>& argv);

	const std::vector<std::string>& arguments() const {
		return arguments_;
	}

	const std::string& argument(uint32_t pos) const {
		return arguments_[pos];
	}

	bool isSet(const std::string& name) const {
		sassert(isOptionExpected(name));
		return options_.at(name);
	}

	bool isOptionExpected(const std::string& name) const {
		return options_.count(name) > 0;
	}

private:
	std::map<std::string, bool> options_;
	std::vector<std::string> arguments_;
};
