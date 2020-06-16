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

#include <signal.h>
#include <stdlib.h>

#include <iostream>
#include <sstream>
#include <string>

#include "devtools/configuration.h"

class UtilsConfiguration : public Configuration {
public:
	static size_t blockSize() {
		return getIntWithUnitOr("BLOCK_SIZE", 64 * 1024);
	}

	static size_t fileSize() {
		return getIntWithUnitOr("FILE_SIZE", 1024 * 1024 * 100);
	}

	static size_t repeatAfter_ms() {
		return getIntOr("REPEAT_AFTER_MS", 0);
	}

	static int seed() {
		return getIntOr("SEED", 0);
	}

public:
	using Configuration::parseIntWithUnit;
	using Configuration::parseInt;
};
