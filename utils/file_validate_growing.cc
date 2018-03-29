/*
   Copyright 2013-2018 Skytechnology sp. z o.o.

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

#include <signal.h>
#include <iostream>
#include <string>
#include "utils/configuration.h"
#include "utils/data_generator.h"

int main(int argc, char** argv) {
	if (argc != 3) {
		std::cerr << "Usage:" << std::endl
				<< "    " << argv[0] << " <file name> <size>" << std::endl;
		return 1;
	}
	signal(SIGPIPE, SIG_IGN);

	try {
		DataGenerator::validateGrowingFile(argv[1], std::stoi(argv[2]));
	} catch (std::exception& ex) {
		std::cerr << "File " << argv[1] << ": " << ex.what() << std::endl;
		return 2;
	}
	return 0;
}

