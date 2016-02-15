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
#include <fcntl.h>
#include <cstdlib>
#include <string>
#include <gtest/gtest.h>

#include "chunkserver/output_buffer.h"
#include "unittests/TemporaryDirectory.h"

TEST(OutputBufferTests, outputBuffersTest) {
	OutputBuffer outputBuffer(512*1024);

	int auxPipeFileDescriptors[2];
	ASSERT_NE(pipe2(auxPipeFileDescriptors, O_NONBLOCK), -1);
#ifdef F_SETPIPE_SZ
	ASSERT_NE(fcntl(auxPipeFileDescriptors[1], F_SETPIPE_SZ, 512*1024), -1);
#endif


	const unsigned WRITE_SIZE = 10;
	unsigned VALUE = 17u;

	uint8_t buf[WRITE_SIZE];
	memset(buf, VALUE, WRITE_SIZE);
	ASSERT_EQ(outputBuffer.copyIntoBuffer(buf, WRITE_SIZE), WRITE_SIZE);

	while (true) {
		OutputBuffer::WriteStatus status = outputBuffer.writeOutToAFileDescriptor(auxPipeFileDescriptors[1]);
		ASSERT_NE(status, OutputBuffer::WRITE_ERROR);
		if (status == OutputBuffer::WRITE_DONE) {
			break;
		}
		sleep(1);
	}

	ASSERT_EQ(read(auxPipeFileDescriptors[0], buf, WRITE_SIZE), WRITE_SIZE) << "errno: " << errno;

	for (unsigned j = 0; j < WRITE_SIZE; ++j) {
		ASSERT_EQ(VALUE, buf[j]) << "Byte " << j << " in block doesn't match";
	}

	close(auxPipeFileDescriptors[0]);
	close(auxPipeFileDescriptors[1]);
}
