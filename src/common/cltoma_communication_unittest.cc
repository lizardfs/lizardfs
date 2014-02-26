#include "common/cltoma_communication.h"

#include <gtest/gtest.h>

#include "unittests/packet.h"
#include "unittests/inout_pair.h"

TEST(CltomaCommunicationTests, IoLimitNeeds) {
	LIZARDFS_DEFINE_INOUT_PAIR(std::string, group       , "group 123", "");
	LIZARDFS_DEFINE_INOUT_PAIR(bool       , wantMore    , true       , false);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t   , currentLimit, 1234       , 0);
	LIZARDFS_DEFINE_INOUT_PAIR(uint64_t   , recentUsage , 5678       , 0);

	std::vector<uint8_t> buffer;
	ASSERT_NO_THROW(cltoma::iolimit::serialize(buffer, groupIn, wantMoreIn,
			currentLimitIn, recentUsageIn));

	verifyHeader(buffer, LIZ_CLTOMA_IOLIMIT);
	removeHeaderInPlace(buffer);
	ASSERT_NO_THROW(cltoma::iolimit::deserialize(buffer.data(), buffer.size(),
			groupOut, wantMoreOut, currentLimitOut, recentUsageOut));

	LIZARDFS_VERIFY_INOUT_PAIR(group);
	LIZARDFS_VERIFY_INOUT_PAIR(wantMore);
	LIZARDFS_VERIFY_INOUT_PAIR(currentLimit);
	LIZARDFS_VERIFY_INOUT_PAIR(recentUsage);
}
