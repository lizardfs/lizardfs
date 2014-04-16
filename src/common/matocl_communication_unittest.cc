#include "common/matocl_communication.h"

#include <gtest/gtest.h>

#include "unittests/packet.h"
#include "unittests/inout_pair.h"

TEST(MatoclCommunicationTests, IoLimitsConfig) {
	std::vector<std::string> groups_tmp{"group 1", "group 20", "group 300"};

	LIZARDFS_DEFINE_INOUT_PAIR(std::string             , subsystem, "cgroups_something", "");
	LIZARDFS_DEFINE_INOUT_VECTOR_PAIR(std::string      , groups) = groups_tmp;
	LIZARDFS_DEFINE_INOUT_PAIR(uint32_t                , frequency, 100                , 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::iolimits_config::serialize(buffer, subsystemIn, groupsIn,
			frequencyIn));

	verifyHeader(buffer, LIZ_MATOCL_IOLIMITS_CONFIG);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::iolimits_config::deserialize(buffer.data(), buffer.size(),
			subsystemOut, groupsOut, frequencyOut));

	LIZARDFS_VERIFY_INOUT_PAIR(subsystem);
	LIZARDFS_VERIFY_INOUT_PAIR(groups);
	LIZARDFS_VERIFY_INOUT_PAIR(frequency);
}

TEST(MatoclCommunicationTests, IoLimitAllocated) {
	LIZARDFS_DEFINE_INOUT_PAIR(std::string, group, "group 123", "");
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t, limit, 1234, 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(matocl::iolimit::serialize(buffer, groupIn, limitIn));

	verifyHeader(buffer, LIZ_MATOCL_IOLIMIT);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(matocl::iolimit::deserialize(buffer.data(), buffer.size(),
			groupOut, limitOut));

	LIZARDFS_VERIFY_INOUT_PAIR(limit);
	LIZARDFS_VERIFY_INOUT_PAIR(group);
}
