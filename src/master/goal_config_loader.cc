#include "common/platform.h"
#include "master/goal_config_loader.h"

#include <iterator>
#include <sstream>
#include <string>

#include "common/exceptions.h"

void GoalConfigLoader::load(std::istream&& stream) {
	GoalMap<Goal> result;
	std::string line;
	uint32_t lineNum = 0;
	while (std::getline(stream, line)) {
		std::string currentPosition = "line " + std::to_string(++lineNum);

		// Remove comments
		auto it = line.find('#');
		if (it != std::string::npos) {
			line.erase(it);
		}

		// Split the line into a vector of tokens
		std::istringstream ss(line);
		std::vector<std::string> tokens = std::vector<std::string>(
				std::istream_iterator<std::string>(ss),
				std::istream_iterator<std::string>());

		// Skip empty lines
		if (tokens.empty()) {
			continue;
		}

		// Now parse a line in the form of, eg:
		//   1 some_name: _ _ ssd
		//  10 10 : _ _ _ _ _ _ _ _ _ _
		//  11    eleven : hdd hdd _ _ hdd
		//  12 tape : _ _ _@ tape@

		// Read ID of the goal
		unsigned long long goalId;
		try {
			goalId = std::stoull(tokens.front());
		} catch (std::exception&) {
			throw ParseException(currentPosition + ": malformed goal ID");
		}
		if (goalId < goal::kMinGoal || goalId > goal::kMaxGoal) {
			throw ParseException(currentPosition + ": goal ID out of range");
		}
		if (!result[goalId].name().empty()) {
			throw ParseException(currentPosition + ": repeated goal ID " + tokens.front());
		}
		tokens.erase(tokens.begin());

		// Read name of the goal
		if (tokens.empty() || tokens.front()[0] == ':') {
			throw ParseException(currentPosition + ": missing name of the goal");
		}
		std::string goalName = tokens.front();
		tokens.erase(tokens.begin());

		// Read the colon -- it might be a separate token or be appended to 'goalName'
		if (goalName[goalName.size() - 1] == ':') {
			goalName.resize(goalName.size() - 1);
			if (goalName.empty()) {
				throw ParseException(currentPosition + ": missing name of the goal");
			}
		} else {
			if (tokens.empty() || tokens.front() != ":") {
				throw ParseException(currentPosition + ": missing colon");
			}
			tokens.erase(tokens.begin());
		}

		// Verify if there are any labels and if they contain only allowed characters
		if (tokens.empty()) {
			throw ParseException(currentPosition + ": missing labels");
		}
		Goal::Labels chunkLabels, tapeLabels;
		uint32_t chunkCopies = 0;
		uint32_t tapeCopies = 0;
		for (const auto& token : tokens) {
			if (token.empty()) {
				throw ParseException(currentPosition + ": empty label ");
			} else {
				if (token.back() == '@') { // tapeserver label
					std::string label = token.substr(0, token.size() - 1);
					if (!isMediaLabelValid(label)) {
						throw ParseException(currentPosition + ": invalid label " + token);
					}
					++tapeLabels[label];
					++tapeCopies;
				} else { // chunkserver label
					if (!isMediaLabelValid(token)) {
						throw ParseException(currentPosition + ": invalid label " + token);
					}
					++chunkLabels[token];
					++chunkCopies;
				}
			}
		}
		// Let's verify number of chunk and tape labels
		if (chunkCopies > Goal::kMaxExpectedChunkCopies
				|| tapeCopies > Goal::kMaxExpectedTapeCopies) {
			throw ParseException(currentPosition + ": too many labels");
		}
		// Let's also verify name of the goal
		if (!Goal::isNameValid(goalName)) {
			throw ParseException(currentPosition + ": invalid name of goal " + goalName);
		}

		result[goalId] = Goal(goalName, std::move(chunkLabels), std::move(tapeLabels));
	}

	if (stream.bad()) {
		throw ParseException("I/O error");
	}

	// Fill all other valid goals with default values
	for (uint8_t goal = goal::kMinGoal; goal <= goal::kMaxGoal; ++goal) {
		if (result[goal].name().empty()) {
			result[goal] = Goal(std::to_string(goal), {{kMediaLabelWildcard, goal}}, {});
		}
	}
	goals_ = std::move(result);
}
