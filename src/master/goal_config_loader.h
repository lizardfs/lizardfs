#pragma once
#include "common/platform.h"

#include <map>

#include "common/goal_map.h"

/// Parser for files with configuration of goals
class GoalConfigLoader {
public:
	/// Reads the configuration file and parses it.
	/// \throws ParseException
	void load(std::istream&& stream);

	/// Returns result of the parsing.
	const GoalMap<Goal>& goals() const {
		return goals_;
	}

private:
	GoalMap<Goal> goals_;
};

