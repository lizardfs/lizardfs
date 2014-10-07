#pragma once
#include "common/platform.h"

#include <cstdint>
#include <string>
#include <vector>

#include "common/media_label.h"

namespace goal {

constexpr uint8_t kMinGoal = 1;
constexpr uint8_t kMaxGoal = 20;
constexpr uint8_t kNumberOfGoals = kMaxGoal - kMinGoal + 1;

inline bool isGoalValid(uint8_t goal) {
	return goal >= kMinGoal && goal <= kMaxGoal;
}

} // namespace goal

/// A class which represents a description of a goal.
struct Goal {
	/// A default constructor.
	Goal() {}

	/// Constructor.
	Goal(std::string name, std::vector<MediaLabel> labels)
			: name(std::move(name)),
			  labels(std::move(labels)) {
	}

	/// Verifies names of goals.
	static bool isNameValid(const std::string& goalName) {
		// Let's use exactly the same algorithm as for media labels
		return isMediaLabelValid(goalName);
	}

	/// Name of the goal.
	std::string name;

	/// For each desired copy, a label of media, where it should be stored.
	std::vector<MediaLabel> labels;
};
