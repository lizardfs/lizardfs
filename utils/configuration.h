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
		return getIntOr("BLOCK_SIZE", 64 * 1024);
	}

	static size_t fileSize() {
		return getIntOr("FILE_SIZE", 1024 * 1024 * 100);
	}
};
