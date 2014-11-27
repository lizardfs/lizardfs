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
