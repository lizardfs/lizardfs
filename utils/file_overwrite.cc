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

#include <string>
#include "utils/configuration.h"
#include "utils/data_generator.h"

int main(int argc, char** argv) {
	if (argc < 2) {
		std::cerr << "Usage:\n"
			"    " << argv[0] << " <file>...\n"
			"Command uses the following environment variables:\n"
			"* BLOCK_SIZE\n"
			"* SEED" << std::endl;
		return 1;
	}

	DataGenerator generator(UtilsConfiguration::seed());
	for (int i = 1; i < argc; ++i) {
		generator.overwriteFile(argv[i]);
	}
	return 0;
}
