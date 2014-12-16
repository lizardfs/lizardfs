#include "common/platform.h"

#include <algorithm>
#include <iostream>

#include "common/human_readable_format.h"
#include "common/MFSCommunication.h"
#include "common/mfserr.h"
#include "admin/chunk_health_command.h"
#include "admin/info_command.h"
#include "admin/io_limits_status_command.h"
#include "admin/list_chunkservers_command.h"
#include "admin/list_disks_command.h"
#include "admin/list_goals_command.h"
#include "admin/list_metadataservers_command.h"
#include "admin/list_mounts_command.h"
#include "admin/metadataserver_status_command.h"
#include "admin/ready_chunkservers_count_command.h"

int main(int argc, const char** argv) {
	strerr_init();
	std::vector<const LizardFsProbeCommand*> allCommands = {
			new ChunksHealthCommand(),
			new InfoCommand(),
			new IoLimitsStatusCommand(),
			new ListChunkserversCommand(),
			new ListDisksCommand(),
			new ListGoalsCommand(),
			new ListMountsCommand(),
			new ListMetadataserversCommand(),
			new MetadataserverStatusCommand(),
			new ReadyChunkserversCountCommand(),
	};

	try {
		if (argc < 2) {
			throw WrongUsageException("No command name provided");
		}
		std::string commandName(argv[1]);
		std::vector<std::string> arguments(argv + 2, argv + argc);
		for (auto command : allCommands) {
			if (command->name() == commandName) {
				try {
					std::vector<std::string> supportedOptions;
					for (const auto& optionWithDescription : command->supportedOptions()) {
						supportedOptions.push_back(optionWithDescription.first);
					}
					command->run(Options(supportedOptions, arguments));
					strerr_term();
					return 0;
				} catch (Options::ParseError& ex) {
					throw WrongUsageException("Wrong usage of " + command->name()
							+ "; " + ex.what());
				}
			}
		}
		throw WrongUsageException("Unknown command " + commandName);
	} catch (WrongUsageException& ex) {
		std::cerr << ex.message() << std::endl;
		std::cerr << "Usage:\n";
		std::cerr << "    " << argv[0] << " COMMAND [OPTIONS...] [ARGUMENTS...]\n\n";
		std::cerr << "Available COMMANDs:\n\n";
		for (auto command : allCommands) {
			command->usage();
			if (!command->supportedOptions().empty()) {
				std::cerr << "    Possible command-line options:\n";
				for (const auto& optionWithDescription : command->supportedOptions()) {
					std::cerr << "\n    " << optionWithDescription.first << "\n";
					std::cerr << "        " << optionWithDescription.second << "\n";
				}
			}
			std::cerr << std::endl;
		}
		strerr_term();
		return 1;
	} catch (Exception& ex) {
		std::cerr << "Error: " << ex.what() << std::endl;
		strerr_term();
		return 1;
	}
}
