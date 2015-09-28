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

#include <cassert>

Options::Options(const std::vector<std::string> &expectedOptions,
		const std::vector<std::string> &argv) {
	// Set expected options
	for (const auto& option : expectedOptions) {
		assert(!option.empty());
		if (option.back() == '=') {
			auto trimmed = option;
			trimmed.pop_back();
			valued_options_[trimmed] = "";
			options_[trimmed] = false;
		} else {
			options_[option] = false;
		}
	}

	bool expecting_value = false;
	std::string valued_option;
	for (const std::string &arg : argv) {
		assert(!arg.empty());

		// If value of an option is expected, assign it
		if (expecting_value) {
			valued_options_[valued_option] = arg;
			expecting_value = false;
			continue;
		}
		if (arg.substr(0, 2) == "--") {
			parseOption(arg, expecting_value, valued_option);
		} else {
			arguments_.push_back(arg);
		}
	}
	if (expecting_value) {
		throw ParseError("Option " + valued_option + " needs an argument");
	}
}

void Options::parseOption(const std::string &arg, bool &expecting_value, std::string &valued_option) {
	size_t separator = arg.find('=');
	if (separator != std::string::npos) {
		auto left = arg.substr(0, separator);
		auto right = arg.substr(separator + 1);
		if (isOptionValued(left)) {
			valued_options_[left] = right;
			options_[left] = true;
			return;
		}
		throw ParseError("Unexpected parameter passed to option " + left);
	}
	if (!isOptionExpected(arg)) {
		throw ParseError("Unexpected option " + arg);
	}
	if (isOptionValued(arg)) {
		valued_option = arg;
		expecting_value = true;
	}
	options_[arg] = true;
}
