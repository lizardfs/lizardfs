/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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
#include "common/wrong_crc_notifier.h"

#include <gtest/gtest.h>

#include "protocol/cltocs.h"
#include "unittests/chunk_type_constants.h"
#include "unittests/mocks/chunk_connector_mock.h"

TEST(WrongCrcNotifierTests, WrongCrcNotifier) {
	class : public ModuleMock {
	protected:
		void onIncomingMessage(uint32_t type, const std::vector<uint8_t>& message) {
			ASSERT_EQ(LIZ_CLTOCS_TEST_CHUNK, type);
			ChunkWithVersionAndType chunk;
			cltocs::testChunk::deserialize(message.data(), message.size(),
					chunk.id, chunk.version, chunk.type);
			EXPECT_EQ(2U, chunk.id);
			EXPECT_EQ(3U, chunk.version);
			EXPECT_EQ(xor_2_of_3, chunk.type);
		}
	} mock;

	NetworkAddress server(1, 1);
	std::unique_ptr<ChunkConnector> connector(new ChunkConnectorMock{{server, &mock}});

	WrongCrcNotifier notifier;
	notifier.init(std::move(connector));
	notifier.reportBadCrc(server, 2, 3, xor_2_of_3);
	EXPECT_TRUE(mock.waitForPacketReceived(std::chrono::seconds(1)));
}
