#pragma once

#include "common/platform.h"

#define LIZARDFS_DEFINE_INOUT_PAIR(type, name, inVal, outVal) \
		type name##Out{outVal}, name##In{inVal}

#define LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(type, name) \
		std::vector<type> name##Out, name##In

#define LIZARDFS_VERIFY_INOUT_PAIR(name) \
		EXPECT_EQ(name##In, name##Out)
