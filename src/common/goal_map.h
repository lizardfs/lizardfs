#pragma once

#include <cstdint>

#include "common/chunk_type.h"
#include "common/exception.h"
#include "common/goal.h"
#include "common/serialization.h"

class GoalMapInvalidGoalException : public Exception {
public:
	GoalMapInvalidGoalException(uint8_t goal) :
		Exception("invalid goal: " + std::to_string(goal)) {
	}
};

template <class T>
class GoalMap;

template <class T>
inline uint32_t serializedSize(const GoalMap<T>& map);
template <class T>
inline void serialize(uint8_t** destination, const GoalMap<T>& map);
template <class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, GoalMap<T>& map);

template <class T>
class GoalMap {
public:
	GoalMap() : ordinary_(), xor_(), zero_() {}

	T& operator[](uint8_t goal) {
		if (isOrdinaryGoal(goal)) {
			return ordinary_[goal - kMinOrdinaryGoal];
		}
		if (isXorGoal(goal)) {
			return xor_[goal - kMinXorGoal];
		}
		if (goal == 0) {
			return zero_;
		}
		throw GoalMapInvalidGoalException(goal);
	}
	const T& operator[](uint8_t goal) const {
		return const_cast<GoalMap&>(*this)[goal];
	}
	friend uint32_t serializedSize<T>(const GoalMap<T>&);
	friend void serialize<T>(uint8_t**, const GoalMap<T>&);
	friend void deserialize<T>(const uint8_t**, uint32_t&, GoalMap<T>&);
private:
	T ordinary_[kMaxOrdinaryGoal - kMinOrdinaryGoal + 1];
	T xor_[kMaxXorGoal - kMinXorGoal + 1];
	T zero_;
};

template <class T>
inline uint32_t serializedSize(const GoalMap<T>& map) {
	return serializedSize(map.ordinary_, map.xor_, map.zero_);
}

template <class T>
inline void serialize(uint8_t** destination, const GoalMap<T>& map) {
	serialize(destination, map.ordinary_, map.xor_, map.zero_);
}

template <class T>
inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer, GoalMap<T>& map) {
	deserialize(source, bytesLeftInBuffer, map.ordinary_, map.xor_, map.zero_);
}
