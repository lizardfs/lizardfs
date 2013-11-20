#include <string>
#include "utils/configuration.h"
#include "utils/data_generator.h"

int main(int argc, char** argv) {
	if (argc == 1) {
		std::cerr << "Usage:" << std::endl
				<< "    " << argv[0] << " <file>" << std::endl
				<< "Command uses the following environment variables: " << std::endl
				<< "* FILE_SIZE" << std::endl
				<< "* BLOCK_SIZE" << std::endl;
		return 1;
	}
	signal(SIGPIPE, SIG_IGN);

	std::string file = argv[1];
	DataGenerator::createFile(file, Configuration::fileSize());

	return 0;
}
