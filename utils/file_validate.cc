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
