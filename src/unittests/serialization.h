/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
#include "common/serialization.h"

#include <gtest/gtest.h>

#include "unittests/inout_pair.h"

template<class T>
void serializeTest(const T& toBeSerialized) {
	LIZARDFS_DEFINE_INOUT_PAIR(T, toBeTested, toBeSerialized, T());

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(serialize(buffer, toBeTestedIn));
	ASSERT_NO_THROW(deserialize(buffer, toBeTestedOut));

	LIZARDFS_VERIFY_INOUT_PAIR(toBeTested);
}
