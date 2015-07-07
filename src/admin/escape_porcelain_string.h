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

#include <string>

std::string escapePorcelainString(std::string string) {
	auto replaceAll = [](std::string& str, const std::string& oldText, const std::string& newText) {
		int replaced = 0;
		size_t pos = 0;
		while ((pos = str.find(oldText, pos)) != std::string::npos) {
			str.replace(pos, oldText.length(), newText);
			pos += newText.length();
			++replaced;
		}
		return replaced;
	};
	int replaced = 0;
	replaced += replaceAll(string, "\\", "\\\\");
	replaced += replaceAll(string, "\"", "\\\"");
	if (string.find(' ') != string.npos || string.empty() || replaced != 0) {
		string = '"' + string + '"';
	}
	return string;
}
