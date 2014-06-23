#include "config.h"
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

