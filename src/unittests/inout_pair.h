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

#define LIZARDFS_DEFINE_INOUT_PAIR(type, name, inVal, outVal) \
		type name##Out{outVal}, name##In{inVal}

#define LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(type, name) \
		std::vector<type> name##Out, name##In

#define LIZARDFS_VERIFY_INOUT_PAIR(name) \
		EXPECT_EQ(name##In, name##Out)
