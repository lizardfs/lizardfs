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

#include <signal.h>
#include <iostream>
#include <string>
#include "utils/data_generator.h"

int main(int argc, char** argv) {
	if (argc == 1) {
		std::cerr << "Usage:" << std::endl
				<< "    " << argv[0] << " <file>..." << std::endl;
		return 1;
	}
	signal(SIGPIPE, SIG_IGN);

	int error = 0;
	for (int i = 1; i < argc; ++i) {
		std::string file = argv[i];
		try {
			DataGenerator::validateFile(file);
		} catch (std::exception& ex) {
			std::cerr << "File " << file << ": " << ex.what() << std::endl;
			error = 2;
		}
	}
	return error;
}
