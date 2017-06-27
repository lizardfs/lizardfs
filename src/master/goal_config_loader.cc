/*
   Copyright 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o.

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
#include "common/slice_traits.h"

namespace goal_config {

namespace {

inline bool isWhiteSpace(char c) {
	return std::isspace(c);
}

inline bool isTokenBreaker(char c) {
	return isWhiteSpace(c) || std::strchr("{}$:#", c);
}

inline bool isCharAllowed(char c) {
	return isTokenBreaker(c) || std::isalnum(c) || std::strchr("_(),", c);
}

std::list<std::string> tokenizeLine(const std::string &str) {
	std::list<std::string> tokens;
	std::string tmp_token;
	for (char c : str) {
		if (!isCharAllowed(c)) {
			throw ParseException("Unexpected character '" + std::string(1, c) + "'");
		}

		if (isTokenBreaker(c)) {
			if (!tmp_token.empty()) {
				tokens.push_back(std::move(tmp_token));
			}

			if (c == '#') {
				break;
			}

			if(!isWhiteSpace(c)) {
				tokens.emplace_back(1, c);
			}
			continue;
		}

		if (!isWhiteSpace(c)) {
			tmp_token.push_back(c);
		}
	}
	if (!tmp_token.empty()) {
		tokens.push_back(std::move(tmp_token));
	}
	return tokens;
}

int parseGoalId(std::list<std::string> &tokens) {
	assert(!tokens.empty());
	int goal_id;
	try {
		std::size_t pos;
		goal_id = std::stoi(tokens.front(), &pos);
		if (pos != tokens.front().length()) {
			throw ParseException("malformed goal ID");
		}
	} catch (std::exception&) {
		throw ParseException("malformed goal ID");
	}
	if (!GoalId::isValid(goal_id)) {
		throw ParseException("goal ID out of range");
	}

	tokens.pop_front();
	return goal_id;
}

std::string parseGoalName(std::list<std::string> &tokens) {
	if (tokens.empty()) {
		throw ParseException("no goal name specified");
	}
	std::string goal_name = tokens.front();
	if (!Goal::isNameValid(goal_name)) {
		throw ParseException("invalid name of goal '" + goal_name + "'");
	}
	tokens.pop_front();

	// Read the colon
	if (tokens.empty() || tokens.front() != ":") {
		throw ParseException("missing colon");
	}
	tokens.pop_front();
	return goal_name;
}

Goal::Slice::Type parseErasureCodeType(const std::string &token) {
	static const char *pattern = "ec(%d,%d)";
	int k, m;
	int parsed = std::sscanf(token.c_str(), pattern, &k, &m);

	if (parsed != 2) {
		throw ParseException("Unknown goal type '" + token + "'");
	}

	if (k < slice_traits::ec::kMinDataCount || k > slice_traits::ec::kMaxDataCount ||
		m < slice_traits::ec::kMinParityCount || m > slice_traits::ec::kMaxParityCount) {
		throw ParseException("Wrong erasure code type '" + token + "'");
	}

	Goal::Slice::Type slice_type = slice_traits::ec::getSliceType(k, m);

	if (!slice_type.isValid()) {
		throw ParseException("Wrong erasure code type '" + token + "'");
	}

	return slice_type;
}

Goal::Slice::Type parseSliceType(std::list<std::string> &tokens) {
	static const std::unordered_map<std::string, Goal::Slice::Type> kSliceTypes ({
		{"std", Goal::Slice::Type(Goal::Slice::Type::kStandard)},
		{"xor2", Goal::Slice::Type(Goal::Slice::Type::kXor2)},
		{"xor3", Goal::Slice::Type(Goal::Slice::Type::kXor3)},
		{"xor4", Goal::Slice::Type(Goal::Slice::Type::kXor4)},
		{"xor5", Goal::Slice::Type(Goal::Slice::Type::kXor5)},
		{"xor6", Goal::Slice::Type(Goal::Slice::Type::kXor6)},
		{"xor7", Goal::Slice::Type(Goal::Slice::Type::kXor7)},
		{"xor8", Goal::Slice::Type(Goal::Slice::Type::kXor8)},
		{"xor9", Goal::Slice::Type(Goal::Slice::Type::kXor9)}
	});

	if (tokens.empty()) {
		throw ParseException("no labels specified");
	}
	Goal::Slice::Type slice_type(Goal::Slice::Type::kStandard);
	if (tokens.front() == "$") {
		tokens.pop_front();
		if (tokens.empty()) {
			throw ParseException("missing goal type after $");
		}
		auto it = kSliceTypes.find(tokens.front());
		if (it == kSliceTypes.end()) {
			slice_type = parseErasureCodeType(tokens.front());
		} else {
			slice_type = it->second;
		}
		tokens.pop_front();

		if (!tokens.empty()) {
			if (tokens.front() != "{") {
				throw ParseException("Unexpected token '" + tokens.front()
						+ "' occurred ('{' character was expected)");
			}
			if (tokens.back() != "}") {
				throw ParseException("Expected '}' character at the end of line");
			}
			tokens.pop_front();
			tokens.pop_back();
		}
	}
	return slice_type;
}

Goal::Slice parseLabels(std::list<std::string> &tokens, Goal::Slice::Type slice_type) {
	Goal::Slice slice(slice_type);
	if (slice_traits::isStandard(slice)) {
		if (tokens.empty()) {
			throw ParseException("no labels");
		}
		while (!tokens.empty()) {
			if (!MediaLabelManager::isLabelValid(tokens.front())) {
				throw ParseException("invalid label '" + tokens.front() + "'");
			}
			++(slice[0][MediaLabel(tokens.front())]);
			tokens.pop_front();
		}
	} else {
		for (auto part : slice) {
			MediaLabel label = MediaLabel::kWildcard;
			if (!tokens.empty()) {
				if (!MediaLabelManager::isLabelValid(tokens.front())) {
					throw ParseException("invalid label '" + tokens.front() + "'");
				}
				label = MediaLabel(tokens.front());
				tokens.pop_front();
			}
			part.insert(std::make_pair(label, 1));
		}
		if (!tokens.empty()) {
			throw ParseException("too many labels for type '" + to_string(slice_type) + "'");
		}
	}
	return slice;
}

} // namespace detail

Goal defaultGoal(int goal_id) {
	int copies = std::min(kMaxCompatibleGoal, goal_id);
	Goal goal(std::to_string(goal_id));
	Goal::Slice slice(Goal::Slice::Type{Goal::Slice::Type::kStandard});
	slice[0][MediaLabel::kWildcard] = copies;
	goal.setSlice(std::move(slice));

	return goal;
}

std::pair<int, Goal> parseLine(const std::string &line) {
	// Split the line into a vector of tokens
	std::list<std::string> tokens = tokenizeLine(line);

	// empty line
	if (tokens.empty()) {
		return {0, Goal()};
	}

	// Now parse a line in the form of, eg:
	//   1 some_name: _ _ ssd
	//  10 10 : _ _ _ _ _ _ _ _ _ _
	//  11    eleven : {hdd hdd _ _ hdd }
	//  12 standard    : $std {_ _ _}
	//  13 xor2 : $xor2 { A A A }
	//  14 xor3 : $xor3{A B C}
	//  15 xor2any : $xor2

	int goal_id = parseGoalId(tokens);

	std::string goal_name = parseGoalName(tokens);
	Goal::Slice::Type slice_type = parseSliceType(tokens);

	// Let's verify number of servers necessary
	if (tokens.size() > Goal::kMaxExpectedCopies) {
		throw ParseException("too many labels (max: "
				+ std::to_string(Goal::kMaxExpectedCopies) + ")");
	}

	Goal::Slice slice = parseLabels(tokens, slice_type);

	Goal goal(goal_name);
	goal.setSlice(slice);
	return std::make_pair(goal_id, goal);
}

std::map<int, Goal> load(std::istream& stream) {
	std::map<int, Goal> goals;
	std::string line;
	for (int line_num = 1; std::getline(stream, line); ++line_num) try {
		auto parsed_line = parseLine(line);
		int goal_id = parsed_line.first;
		if (goal_id) {
			if (goals.find(goal_id) != goals.end()) {
				throw ParseException("repeated goal ID " + std::to_string(goal_id));
			}
			goals.insert(std::move(parsed_line));
		}
	} catch (ParseException &e) {
		throw ParseException(line_num, e.message());
	}

	if (stream.bad()) {
		throw ParseException("I/O error");
	}

	// Fill all other valid goals with default values
	for (int goal_id = GoalId::kMin; goal_id <= GoalId::kMax; ++goal_id) {
		auto it = goals.find(goal_id);
		if (it == goals.end()) {
			goals[goal_id] = defaultGoal(goal_id);
		}
	}

	return goals;
}

std::map<int, Goal> load(std::istream&& stream) {
	return load(stream);
}

} // namespace goal_config
