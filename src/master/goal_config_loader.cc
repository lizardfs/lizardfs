/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
		if (goalId < goal::kMinOrdinaryGoal || goalId > goal::kMaxOrdinaryGoal) {
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
					if (!MediaLabelManager::isLabelValid(label)) {
						throw ParseException(currentPosition + ": invalid label " + token);
					}
					++tapeLabels[MediaLabel(label)];
					++tapeCopies;
				} else { // chunkserver label
					if (!MediaLabelManager::isLabelValid(token)) {
						throw ParseException(currentPosition + ": invalid label " + token);
					}
					++chunkLabels[MediaLabel(token)];
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
	for (uint8_t goal = goal::kMinOrdinaryGoal; goal <= goal::kMaxOrdinaryGoal; ++goal) {
		if (result[goal].name().empty()) {
			result[goal] = Goal(std::to_string(goal), {{MediaLabel::kWildcard, goal}}, std::map<MediaLabel, int>{});
		}
	}
	goals_ = std::move(result);
}
