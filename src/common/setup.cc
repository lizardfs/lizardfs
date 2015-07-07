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
#include "common/setup.h"

#include <locale>
#include <cstdlib>

void prepareEnvironment() {
	const char* localeNames[] = {
		/*
		 * :r !locale | sed -e 's@\([A-Z_]\+\)=.*@"\1",@'
		 */
		"LANG",
		"LANGUAGE",
		"LC_CTYPE",
		"LC_NUMERIC",
		"LC_TIME",
		"LC_COLLATE",
		"LC_MONETARY",
		"LC_MESSAGES",
		"LC_PAPER",
		"LC_NAME",
		"LC_ADDRESS",
		"LC_TELEPHONE",
		"LC_MEASUREMENT",
		"LC_IDENTIFICATION",
		"LC_ALL"
	};
	try {
		/*
		 * Verify that system's current locale settings are correct.
		 */
		std::locale l("");
	} catch (...) {
		for (auto ln : localeNames) {
			unsetenv(ln);
		}
	}
	return;
}

int gVerbosity = 0;

