/*
   Copyright 2013-2017 Skytechnology sp. z o.o.

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
#include "common/chunk_copies_calculator.h"

#include <algorithm>
#include <gtest/gtest.h>

#include "common/goal.h"
#include "common/slice_traits.h"
#include "master/goal_config_loader.h"
#include "unittests/chunk_type_constants.h"


#define sneakyPartType(type) \
	Goal::Slice::Type(Goal::Slice::Type:: type)

TEST(ChunkCopiesCalculator, addPart) {
	Goal goal = goal_config::parseLine("1 goalname: $xor3 {A B B C}\n").second;
	ChunkCopiesCalculator cccp(goal);
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 3), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 2), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 0), MediaLabel("A"));
	Goal &avalible = cccp.getAvailable();
	avalible.setName("goalname");

	ASSERT_EQ(avalible, goal);

	cccp.addPart(slice_traits::xors::ChunkPartType(3, 0), MediaLabel("A"));

	ASSERT_NE(avalible, goal);
}

TEST(ChunkCopiesCalculator, removePart) {
	Goal goal = goal_config::parseLine("1 goalname: $xor3 {A B B C}\n").second;
	ChunkCopiesCalculator cccp(goal);
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 3), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 2), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 0), MediaLabel("A"));
	Goal &avalible = cccp.getAvailable();
	avalible.setName("goalname");

	ASSERT_EQ(avalible, goal);

	cccp.removePart(sneakyPartType(kXor3), 0, MediaLabel("A"));
	cccp.removePart(sneakyPartType(kXor3), 1, MediaLabel("B"));
	cccp.removePart(sneakyPartType(kXor3), 2, MediaLabel("B"));
	cccp.removePart(sneakyPartType(kXor3), 3, MediaLabel("C"));

	auto slice = cccp.getAvailable().find(sneakyPartType(kXor3));

	ASSERT_TRUE(slice != cccp.getAvailable().end());

	for (auto  it = slice->begin(); it != slice->end(); it++) {
		ASSERT_EQ((*it).size(), 0U);
	}
}

TEST(ChunkCopiesCalculator, getState) {
	Goal goal = goal_config::parseLine("1 gxor4: $xor4 {A B B B C}\n").second;
	Goal goal2 = goal_config::parseLine("2 gstandard: A B\n").second;

	Goal melt;
	melt.mergeIn(goal);
	melt.mergeIn(goal2);

	ChunkCopiesCalculator cccp(melt);
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 0), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 3), MediaLabel("A"));

	cccp.evalRedundancyLevel();
	ASSERT_EQ(ChunksAvailabilityState::State::kEndangered, cccp.getState());

	cccp.addPart(slice_traits::standard::ChunkPartType(), MediaLabel("A"));
	cccp.evalRedundancyLevel();
	ASSERT_EQ(ChunksAvailabilityState::State::kSafe, cccp.getState());

	cccp.removePart(Goal::Slice::Type(Goal::Slice::Type::kStandard), 0,
				MediaLabel("A"));
	cccp.evalRedundancyLevel();
	ASSERT_EQ(ChunksAvailabilityState::State::kEndangered, cccp.getState());

	cccp.removePart(sneakyPartType(kXor4), 1, MediaLabel("B"));
	cccp.removePart(sneakyPartType(kXor4), 1, MediaLabel("A"));
	cccp.evalRedundancyLevel();
	ASSERT_EQ(ChunksAvailabilityState::State::kEndangered, cccp.getState());

	cccp.removePart(sneakyPartType(kXor4), 1, MediaLabel("B"));
	cccp.evalRedundancyLevel();
	ASSERT_EQ(ChunksAvailabilityState::State::kLost, cccp.getState());


	cccp.addPart(slice_traits::xors::ChunkPartType(2, 0), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(2, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(2, 2), MediaLabel("A"));
	cccp.evalRedundancyLevel();

	ASSERT_EQ(ChunksAvailabilityState::State::kSafe, cccp.getState());
}


TEST(ChunkCopiesCalculator, evalRedundancyLevel) {
	Goal goal = goal_config::parseLine("1 gxor4: $xor4 {A B B B C}\n").second;
	Goal goal2 = goal_config::parseLine("2 gstandard: A B\n").second;

	Goal melt;
	melt.mergeIn(goal);
	melt.mergeIn(goal2);

	ChunkCopiesCalculator cccp(melt);
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 0), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 3), MediaLabel("B"));

	cccp.evalRedundancyLevel();
	ASSERT_EQ(0, cccp.getRedundancyLevel());

	cccp.addPart(slice_traits::standard::ChunkPartType(), MediaLabel("A"));
	cccp.evalRedundancyLevel();
	ASSERT_EQ(1, cccp.getRedundancyLevel());

	cccp.addPart(slice_traits::standard::ChunkPartType(), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("C"));
	cccp.evalRedundancyLevel();
	ASSERT_EQ(3, cccp.getRedundancyLevel());

	cccp.addPart(slice_traits::standard::ChunkPartType(), MediaLabel("B"));
	cccp.addPart(slice_traits::standard::ChunkPartType(), MediaLabel("B"));
	cccp.evalRedundancyLevel();
	ASSERT_EQ(5, cccp.getRedundancyLevel());
}

TEST(ChunkCopiesCalculator, optimize) {
	Goal goal = goal_config::parseLine("1 goalname: $xor5 {A B _ B C _}\n").second;

	ChunkCopiesCalculator cccp(goal);
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 0), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 1), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 2), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 2), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 3), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 3), MediaLabel("X"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 4), MediaLabel("X"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 5), MediaLabel("A"));

	Goal &avalible = cccp.getAvailable();
	avalible.setName("goalname");

	cccp.optimize();
	int cost = cccp.countPartsToRecover();

	ASSERT_EQ(1, cost) << "result = " << to_string(cccp.getTarget()) << " ops=(" << cccp.countPartsToRecover() << "," << cccp.countPartsToRemove() << ")";
}

TEST(ChunkCopiesCalculator, updateRedundancyLevel) {
	Goal goal = goal_config::parseLine("1 goalname: $xor3 {A B B C}\n").second;
	ChunkCopiesCalculator cccp(goal);
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 3), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 2), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(3, 0), MediaLabel("A"));
	Goal &avalible = cccp.getAvailable();
	avalible.setName("goalname");

	cccp.evalRedundancyLevel();
	ASSERT_EQ(ChunksAvailabilityState::State::kSafe, cccp.getState());

	cccp.removePart(sneakyPartType(kXor3), 0, MediaLabel("A"));

	cccp.updateRedundancyLevel(sneakyPartType(kXor3));
	ASSERT_EQ(ChunksAvailabilityState::State::kEndangered, cccp.getState());

	cccp.updateRedundancyLevel(sneakyPartType(kXor3));
	cccp.removePart(sneakyPartType(kXor3), 1, MediaLabel("B"));

	cccp.updateRedundancyLevel(sneakyPartType(kXor3));
	ASSERT_EQ(ChunksAvailabilityState::State::kLost, cccp.getState());

	cccp.removePart(sneakyPartType(kXor3), 2, MediaLabel("B"));
	cccp.removePart(sneakyPartType(kXor3), 3, MediaLabel("C"));

	cccp.updateRedundancyLevel(sneakyPartType(kXor3));
	ASSERT_EQ(ChunksAvailabilityState::State::kLost, cccp.getState());
}

TEST(ChunkCopiesCalculator, canRemoveExtraPartsFromSliceSimple) {
	Goal simple = goal_config::parseLine("1 goalname: A\n").second;
	ChunkCopiesCalculator calculator(simple);
	calculator.addPart(slice_traits::standard::ChunkPartType(), MediaLabel("B"));

	calculator.optimize();
	ASSERT_EQ(ChunksAvailabilityState::State::kEndangered, calculator.getState());

	ASSERT_FALSE(calculator.canRemovePart(sneakyPartType(kStandard), 0, MediaLabel("B")));

	calculator.addPart(slice_traits::standard::ChunkPartType(), MediaLabel("A"));
	calculator.evalRedundancyLevel();
	ASSERT_EQ(ChunksAvailabilityState::State::kSafe, calculator.getState());

	// We shouldn't drop to endangered state, but...
	// slice == Slice::Part::Type::kStandard x 1
	// so user definitely requested a single copy.
	// This leads to endangered state by default.
	ASSERT_TRUE(calculator.canRemovePart(sneakyPartType(kStandard), 0, MediaLabel("B")));
	ASSERT_TRUE(calculator.canRemovePart(sneakyPartType(kStandard), 0, MediaLabel("A")));
}


TEST(ChunkCopiesCalculator, canRemoveExtraPartsFromSlice) {
	Goal goal = goal_config::parseLine("1 goalname: $xor5 {A B B C C A}\n").second;
	ChunkCopiesCalculator cccp(goal);
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 0), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 1), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 2), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 3), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 4), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(5, 5), MediaLabel("C"));

	Goal &avalible = cccp.getAvailable();
	avalible.setName("goalname");

	cccp.optimize();
	// goalname: $xor5 {C A A B B C}
	ASSERT_EQ(ChunksAvailabilityState::State::kSafe, cccp.getState());

	// we should never ever drop to endangered state
	// (with an exception of slice == Slice::Part::Type::kStandard x 1)
	ASSERT_FALSE(cccp.canRemovePart(sneakyPartType(kXor5), 0, MediaLabel("C")));
	ASSERT_FALSE(cccp.canRemovePart(sneakyPartType(kXor5), 1, MediaLabel("A")));

	cccp.addPart(slice_traits::xors::ChunkPartType(5, 3), MediaLabel("B"));

	cccp.addPart(slice_traits::xors::ChunkPartType(5, 4), MediaLabel("B"));

	cccp.updateRedundancyLevel(sneakyPartType(kXor5));

	// now we shouldn't drop to endangered state if we remove extra parts
	ASSERT_FALSE(cccp.canRemovePart(sneakyPartType(kXor5), 0, MediaLabel("C")));
	ASSERT_TRUE(cccp.canRemovePart(sneakyPartType(kXor5), 3, MediaLabel("B")));
	ASSERT_TRUE(cccp.canRemovePart(sneakyPartType(kXor5), 4, MediaLabel("B")));

	cccp.removePart(sneakyPartType(kXor5), 5, MediaLabel("C"));

	cccp.updateRedundancyLevel(sneakyPartType(kXor5));

	// now we are in endangered state
	ASSERT_FALSE(cccp.canRemovePart(sneakyPartType(kXor5), 3, MediaLabel("B")));
	ASSERT_FALSE(cccp.canRemovePart(sneakyPartType(kXor5), 4, MediaLabel("B")));
}


TEST(ChunkCopiesCalculator, getLabelsToRecover) {
	Goal goal = goal_config::parseLine("1 goalname: $xor4 {A B B C A}\n").second;
	ChunkCopiesCalculator cccp(goal);
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 0), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 0), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 3), MediaLabel("C"));

	Goal &avalible = cccp.getAvailable();
	avalible.setName("goalname");

	cccp.optimize();
	//goalname: $xor4 {A B A C B}

	Goal::Slice::Labels labels0 = cccp.getLabelsToRecover(sneakyPartType(kXor4), 0);
	Goal::Slice::Labels labels1 = cccp.getLabelsToRecover(sneakyPartType(kXor4), 1);
	Goal::Slice::Labels labels2 = cccp.getLabelsToRecover(sneakyPartType(kXor4), 2);
	Goal::Slice::Labels labels3 = cccp.getLabelsToRecover(sneakyPartType(kXor4), 3);
	Goal::Slice::Labels labels4 = cccp.getLabelsToRecover(sneakyPartType(kXor4), 4);

	ASSERT_EQ(labels0.size(), 0U);
	ASSERT_EQ(labels1.size(), 0U);
	ASSERT_EQ(labels2.size(), 0U);
	ASSERT_EQ(labels3.size(), 0U);

	ASSERT_EQ(labels4.size(), 1U);
	ASSERT_EQ(labels4[MediaLabel("B")], 1);
}


TEST(ChunkCopiesCalculator, getRemovePool) {
	Goal goal = goal_config::parseLine("1 goalname: $xor4 {A A B B B}\n").second;
	ChunkCopiesCalculator cccp(goal);
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 0), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 3), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("C"));

	cccp.evalRedundancyLevel();

	Goal &avalible = cccp.getAvailable();
	avalible.setName("goalname");

	cccp.updateRedundancyLevel(sneakyPartType(kXor4));
	ASSERT_EQ(ChunksAvailabilityState::State::kSafe, cccp.getState());

	cccp.optimize();
	// target: $xor4 {B A B A B}
	Goal::Slice::Labels labels;

	labels = cccp.getRemovePool(sneakyPartType(kXor4), 0);
	ASSERT_EQ(labels.size(), 1U);
	ASSERT_EQ(labels[MediaLabel("A")], 1);

	labels = cccp.getRemovePool(sneakyPartType(kXor4), 1);
	ASSERT_EQ(labels.size(), 0U);

	labels = cccp.getRemovePool(sneakyPartType(kXor4), 2);
	ASSERT_EQ(labels.size(), 1U);
	ASSERT_EQ(labels[MediaLabel("A")], 1);

	labels = cccp.getRemovePool(sneakyPartType(kXor4), 3);
	ASSERT_EQ(labels.size(), 0U);

	labels = cccp.getRemovePool(sneakyPartType(kXor4), 4);
	ASSERT_EQ(labels.size(), 2U);
	ASSERT_EQ(labels[MediaLabel("A")], 1);
	ASSERT_EQ(labels[MediaLabel("C")], 1);
}

TEST(ChunkCopiesCalculator, countPartsToMove) {
	Goal goal = goal_config::parseLine("1 goalname: $xor4 {A A B B C}\n").second;
	ChunkCopiesCalculator cccp(goal);
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 0), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 1), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 2), MediaLabel("B"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 3), MediaLabel("C"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("A"));
	cccp.addPart(slice_traits::xors::ChunkPartType(4, 4), MediaLabel("C"));

	Goal &avalible = cccp.getAvailable();
	avalible.setName("goalname");

	cccp.updateRedundancyLevel(sneakyPartType(kXor4));

	cccp.optimize();
	// target: $xor4 {A B B C A}

	ASSERT_EQ(std::make_pair(1, 1), cccp.countPartsToMove(sneakyPartType(kXor4), 0));
	ASSERT_EQ(std::make_pair(0, 2), cccp.countPartsToMove(sneakyPartType(kXor4), 1));
	ASSERT_EQ(std::make_pair(0, 2), cccp.countPartsToMove(sneakyPartType(kXor4), 2));
	ASSERT_EQ(std::make_pair(0, 0), cccp.countPartsToMove(sneakyPartType(kXor4), 3));
	ASSERT_EQ(std::make_pair(0, 2), cccp.countPartsToMove(sneakyPartType(kXor4), 4));
}
