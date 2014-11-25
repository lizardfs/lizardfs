#pragma once
#include "common/platform.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <numeric>
#include <string>

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
class Goal {
public:
	typedef std::map<MediaLabel, int> Labels;

	/// Maximum number of copies a goal can require.
	static constexpr uint32_t kMaxExpectedCopies = 30;

	/// A default constructor.
	Goal() : size_(0) {}

	/// Constructor.
	Goal(std::string name, Labels labels)
			: name_(std::move(name)),
			  labels_(std::move(labels)),
			  size_(std::accumulate(labels_.begin(), labels_.end(), 0,
				      [](int sum, const Labels::value_type& elem){ return sum + elem.second; })) {
	}

	/// Number of labels in this goal.
	uint32_t getExpectedCopies() const {
		return size_;
	}

	/// Verifies names of goals.
	static bool isNameValid(const std::string& goalName) {
		// Let's use exactly the same algorithm as for media labels
		return isMediaLabelValid(goalName);
	}

	/// Get labels of this goal object.
	const Labels& labels() const {
		return labels_;
	}

	/// Get name of this goal.
	const std::string& name() const {
		return name_;
	}

	/// Oerator ==.
	bool operator==(const Goal& other) {
		return (name_ == other.name_ && labels_ == other.labels_);
	}

private:
	/// Name of the goal.
	std::string name_;

	/// For each desired copy, a label of media, where it should be stored.
	Labels labels_;
	int size_;
};
