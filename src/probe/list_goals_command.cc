#include "common/platform.h"
#include "probe/list_goals_command.h"

#include <iomanip>
#include <iostream>
#include <vector>

#include "common/cltoma_communication.h"
#include "common/goal.h"
#include "common/matocl_communication.h"
#include "common/serialization_macros.h"
#include "common/serialized_goal.h"
#include "common/server_connection.h"
#include "probe/escape_porcelain_string.h"

std::string ListGoalsCommand::name() const {
	return "list-goals";
}

LizardFsProbeCommand::SupportedOptions ListGoalsCommand::supportedOptions() const {
	return {
		{kPorcelainMode, kPorcelainModeDescription},
		{"--pretty", "Print nice table"}
	};
}

void ListGoalsCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>\n";
	std::cerr << "    List goal definitions.\n";
}

void ListGoalsCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.argument(0), options.argument(1));
	std::vector<SerializedGoal> serializedGoals;
	std::vector<uint8_t> request, response;
	cltoma::listGoals::serialize(request, true);
	response = connection.sendAndReceive(request, LIZ_MATOCL_LIST_GOALS);
	matocl::listGoals::deserialize(response, serializedGoals);

	auto goalIdToString = [](uint8_t goalId) {
		return goal::isOrdinaryGoal(goalId) ? std::to_string(goalId) : std::string("-");
	};

	if (options.isSet(kPorcelainMode)) {
		for (const SerializedGoal& goal : serializedGoals) {
			std::cout << goalIdToString(goal.id) << " "
					<< goal.name << " "
					<< escapePorcelainString(goal.definition) << std::endl;
		}
	} else if (options.isSet("--pretty")) {
		std::cout << "Goal definitions:" << std::endl;
		int maxNameLength = std::max<int>(
				std::max_element(
					serializedGoals.begin(),
					serializedGoals.end(),
					[](const SerializedGoal& a, const SerializedGoal& b) {
						return a.name.length() < b.name.length();
					}
				)->name.length(),
				sizeof ("Name") - 1
			);
		int maxDefinitionLength = std::max<int>(
				std::max_element(
					serializedGoals.begin(),
					serializedGoals.end(),
					[](const SerializedGoal& a, const SerializedGoal& b) {
						return a.definition.length() < b.definition.length();
					}
				)->definition.length(),
				sizeof ("Definition") - 1
			);
		std::string frame = "----+-" + std::string(maxNameLength, '-')
			+ "-+-" + std::string(maxDefinitionLength + 1, '-');
		std::cout << frame << std::endl;
		std::cout << " Id | " << std::setw(maxNameLength)
			<< std::left << "Name" << " | Definition" << std::endl;
		std::cout << frame << std::endl;
		for (const SerializedGoal& goal : serializedGoals) {
			std::cout << std::setw(3) << std::right << goalIdToString(goal.id) << " | "
					<< std::setw(maxNameLength) << std::left << goal.name << " | "
					<< goal.definition << std::endl;
		}
		std::cout << frame << std::endl;
	} else {
		std::cout << "Goal definitions:\nId\tName\tDefinition" << std::endl;
		for (const SerializedGoal& goal : serializedGoals) {
			std::cout << goalIdToString(goal.id) << "\t"
					<< goal.name << "\t"
					<< goal.definition << std::endl;
		}
	}
}

