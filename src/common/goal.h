#pragma once

#include "common/platform.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <numeric>
#include <string>

#include "common/chunk_type.h"
#include "common/media_label.h"

namespace goal {

constexpr uint8_t kMinOrdinaryGoal = 1;
constexpr uint8_t kMaxOrdinaryGoal = 20;
constexpr uint8_t kMinXorGoal = 247;
constexpr uint8_t kMaxXorGoal = 255;
constexpr uint8_t kMinXorLevel = 2;
constexpr uint8_t kMaxXorLevel = 10;
constexpr uint8_t kNumberOfOrdinaryGoals = kMaxOrdinaryGoal - kMinOrdinaryGoal + 1;
constexpr uint8_t kNumberOfXorGoals = kMaxXorGoal - kMinXorGoal + 1;

ChunkType::XorLevel toXorLevel(uint8_t goal);
bool isGoalValid(uint8_t goal);
bool isOrdinaryGoal(uint8_t goal);
bool isXorGoal(uint8_t goal);
uint8_t xorLevelToGoal(ChunkType::XorLevel xorLevel);
const std::vector<uint8_t>& allGoals();

} // namespace goal

/// A class which represents a description of a goal.
class Goal {
public:
	typedef std::map<MediaLabel, int> Labels;

	/// Maximum number of copies a goal can require.
	static constexpr uint32_t kMaxExpectedCopies = 30;

	/// A default constructor.
	Goal() : isXor_(false), xorLevel_(0), size_(0) {}

	/// Constructor.
	Goal(std::string name, Labels labels)
			: isXor_(false),
			  xorLevel_(0),
			  name_(std::move(name)),
			  labels_(std::move(labels)),
			  size_(std::accumulate(labels_.begin(), labels_.end(), 0,
				      [](int sum, const Labels::value_type& elem){ return sum + elem.second; })) {
	}

	static Goal getXorGoal(ChunkType::XorLevel level) {
		Goal g;
		g.isXor_ = true;
		g.xorLevel_ = level;
		g.size_ = level + 1;
		g.name_ = "xor" + std::to_string(uint16_t(level));
		return g;
	}

	static Goal getDefaultGoal(uint8_t goalId) {
		if (goal::isXorGoal(goalId)) {
			return Goal::getXorGoal(goal::toXorLevel(goalId));
		} else {
			sassert(goalId == 0 || goal::isOrdinaryGoal(goalId));
			return Goal(std::to_string(goalId), {{kMediaLabelWildcard, goalId}});
		}
	}

	bool isXor() const {
		return isXor_;
	}

	ChunkType::XorLevel xorLevel() const {
		sassert(isXor_);
		return xorLevel_;
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
		return isXor_ == other.isXor_
				&& xorLevel_ == other.xorLevel_
				&& name_ == other.name_
				&& labels_ == other.labels_;
	}

private:
	bool isXor_;
	ChunkType::XorLevel xorLevel_;

	/// Name of the goal.
	std::string name_;

	/// For each desired copy, a label of media, where it should be stored.
	Labels labels_;
	int size_;
};
