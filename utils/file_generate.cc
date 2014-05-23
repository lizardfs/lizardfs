#include <string>
#include "utils/configuration.h"
#include "utils/data_generator.h"

int main(int argc, char** argv) {
	if (argc < 2) {
		std::cerr << "Usage:" << std::endl
				<< "    " << argv[0] << " <file>..." << std::endl
				<< "Command uses the following environment variables: " << std::endl
				<< "* FILE_SIZE" << std::endl
				<< "* BLOCK_SIZE" << std::endl;
		return 1;
	}

	for (int i = 1; i < argc; ++i) {
		DataGenerator::createFile(argv[i], Configuration::fileSize());
	}
	return 0;
}
