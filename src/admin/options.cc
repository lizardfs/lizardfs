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

#include "common/platform.h"
#include "admin/options.h"

Options::Options(const std::vector<std::string>& expectedOptions,
		const std::vector<std::string>& argv) {
	// Set expected options
	for (const auto& option : expectedOptions) {
		options_[option] = false;
	}

	// Set some to true using provided argv
	for (const std::string& arg : argv) {
		if (arg.substr(0, 2) == "--") {
			if (!isOptionExpected(arg)) {
				throw ParseError("Unexpected option " + arg);
			}
			options_[arg] = true;
		} else {
			arguments_.push_back(arg);
		}
	}
}
