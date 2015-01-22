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
	static constexpr uint32_t kMaxExpectedChunkCopies = 30;
	static constexpr uint32_t kMaxExpectedTapeCopies = 30;

	/// A default constructor.
	Goal() : size_(0) {}

	/// Constructor.
	Goal(std::string name, Labels chunkLabels, Labels tapeLabels)
			: name_(std::move(name)),
			  chunkLabels_(std::move(chunkLabels)),
			  tapeLabels_(std::move(tapeLabels)),
			  size_(std::accumulate(chunkLabels_.begin(), chunkLabels_.end(), 0,
				      [](int sum, const Labels::value_type& elem){ return sum + elem.second; })) {
	}

	/// Constructor with no tape labels
	Goal(std::string name, Labels chunkLabels)
		: name_(std::move(name)),
		  chunkLabels_(std::move(chunkLabels)),
		  size_(std::accumulate(chunkLabels_.begin(), chunkLabels_.end(), 0,
				  [](int sum, const Labels::value_type& elem){ return sum + elem.second; })) {
	}

	/// Number of labels to be stored on chunkservers in this goal.
	uint32_t getExpectedCopies() const {
		return size_;
	}

	/// Verifies names of goals.
	static bool isNameValid(const std::string& goalName) {
		// Let's use exactly the same algorithm as for media labels
		return isMediaLabelValid(goalName);
	}

	/// Get labels regarding chunkservers of this goal object.
	const Labels& chunkLabels() const {
		return chunkLabels_;
	}

	/// Get labels regarding tapeservers of this goal object.
	const Labels& tapeLabels() const {
		return tapeLabels_;
	}

	/// Get name of this goal.
	const std::string& name() const {
		return name_;
	}

	/// Operator ==.
	bool operator==(const Goal& other) {
		return (name_ == other.name_ && chunkLabels_ == other.chunkLabels_
				&& tapeLabels_ == other.tapeLabels_);
	}

private:
	/// Name of the goal.
	std::string name_;

	/// For each desired copy, a label of media, where it should be stored.
	Labels chunkLabels_;
	Labels tapeLabels_;
	// Number of copies to be stored on chunkservers
	int size_;
};
