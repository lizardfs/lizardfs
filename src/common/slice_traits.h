/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#pragma once

#include "common/platform.h"

#include <cassert>

#include "common/goal.h"

namespace slice_traits {

inline bool isStandard(Goal::Slice::Type type) {
	return (int)type == Goal::Slice::Type::kStandard;
}

inline bool isStandard(const Goal::Slice &slice) {
	return isStandard(slice.getType());
}

inline bool isTape(Goal::Slice::Type type) {
	return (int)type == Goal::Slice::Type::kTape;
}

inline bool isTape(const Goal::Slice &slice) {
	return isTape(slice.getType());
}

inline bool isXor(Goal::Slice::Type type) {
	int value = (int)type;
	return value >= Goal::Slice::Type::kXor2 && value <= Goal::Slice::Type::kXor9;
}

inline bool isXor(const ::Goal::Slice &slice) {
	return isXor(slice.getType());
}

namespace standard {

} // standard

namespace xors {

constexpr int kXorParityPart = 0;
constexpr int kMinXorLevel   = 2;
constexpr int kMaxXorLevel   = 9;

inline int getXorLevel(Goal::Slice::Type type) {
	assert(::slice_traits::isXor(type));
	return (int)type - (int)Goal::Slice::Type::kXor2 + kMinXorLevel;
}

inline int getXorLevel(const ::Goal::Slice &slice) {
	return getXorLevel(slice.getType());
}

} // xors

}
