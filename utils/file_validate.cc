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
		off_t offset = DataGenerator::validateFile(file);
		if (offset != -1) {
			std::cerr << "Data at offset " << offset
					<< " in file " << file << " corrupted" << std::endl;
			error = 2;
		}
	}
	return error;
}
