#pragma once

#include "common/platform.h"

#include <cstdint>

#include "common/chunk_type.h"

namespace goal {

const uint8_t kMinOrdinaryGoal = 1;
const uint8_t kMaxOrdinaryGoal = 9;
const uint8_t kMinXorGoal = 247;
const uint8_t kMaxXorGoal = 255;
const uint8_t kMinXorLevel = 2;
const uint8_t kMaxXorLevel = 10;

ChunkType::XorLevel toXorLevel(uint8_t goal);
bool isGoalValid(uint8_t goal);
bool isOrdinaryGoal(uint8_t goal);
bool isXorGoal(uint8_t goal);
uint8_t xorLevelToGoal(ChunkType::XorLevel xorLevel);

}

