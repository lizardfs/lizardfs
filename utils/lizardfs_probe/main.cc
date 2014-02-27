#include <algorithm>
#include <iostream>

#include "common/human_readable_format.h"
#include "common/MFSCommunication.h"
#include "utils/lizardfs_probe/chunk_health_command.h"
#include "utils/lizardfs_probe/list_chunkservers_command.h"
#include "utils/lizardfs_probe/ready_chunkservers_count_command.h"

int main(int argc, const char** argv) {
	std::vector<const LizardFsProbeCommand*> allCommands = {
			new ReadyChunkserversCountCommand(),
			new ListChunkserversCommand(),
			new ChunksHealthCommand(),
	};

	try {
		if (argc < 2) {
			throw WrongUsageException("No command name provided");
		}
		std::string commandName(argv[1]);
		std::vector<std::string> arguments(argv + 2, argv + argc);
		for (auto command : allCommands) {
			if (command->name() == commandName) {
				command->run(arguments);
				return 0;
			}
		}
		throw WrongUsageException("Unknown command " + commandName);
	} catch (WrongUsageException& ex) {
		std::cerr << ex.message() << std::endl;
		std::cerr << "Usage:\n";
		std::cerr << "    " << argv[0] << " COMMAND [ARGUMENTS...]\n\n";
		std::cerr << "Available COMMANDs:\n\n";
		for (auto command : allCommands) {
			command->usage();
			std::cerr << std::endl;
		}
		return 1;
	} catch (Exception& ex) {
		std::cerr << "Error: " << ex.what() << std::endl;
		return 1;
	}
}
