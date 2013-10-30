#include "chunkserver/output_buffers.h"

#include <cstdlib>
#include <fcntl.h>
#include <string>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>
#include <gtest/gtest.h>

#include "tests/common/TemporaryDirectory.h"

TEST(OutputBufferTests, outputBuffersTest) {
	std::vector<std::shared_ptr<OutputBuffer>> outputBuffers = {
			std::shared_ptr<OutputBuffer>(new SimpleOutputBuffer(512*1024)),
			std::shared_ptr<OutputBuffer>(new AvoidingCopyingOutputBuffer(512*1024)),
	};

	int auxPipeFileDescriptors[2];
	ASSERT_NE(pipe2(auxPipeFileDescriptors, O_NONBLOCK), -1);
	ASSERT_NE(fcntl(auxPipeFileDescriptors[1], F_SETPIPE_SZ, 512*1024), -1);

	for (unsigned bufferNumber = 0; bufferNumber < outputBuffers.size(); ++bufferNumber) {
		OutputBuffer* outputBuffer = outputBuffers[bufferNumber].get();

		const unsigned WRITE_SIZE = 10;
		unsigned VALUE = 17u;

		uint8_t buf[WRITE_SIZE];
		memset(buf, VALUE, WRITE_SIZE);
		ASSERT_EQ(outputBuffer->copyIntoBuffer(buf, WRITE_SIZE), WRITE_SIZE);

		while (true) {
			OutputBuffer::WriteStatus status = outputBuffer->writeOutToAFileDescriptor(auxPipeFileDescriptors[1]);
			ASSERT_NE(status, OutputBuffer::WRITE_ERROR);
			if (status == OutputBuffer::WRITE_DONE) {
				break;
			}
			sleep(1);
		}

		ASSERT_EQ(read(auxPipeFileDescriptors[0], buf, WRITE_SIZE), WRITE_SIZE) << "errno: " << errno;

		for (unsigned j = 0; j < WRITE_SIZE; ++j) {
			ASSERT_EQ(VALUE, buf[j]) << "Byte " << j << " in block doesn't match for buffer " << bufferNumber;
		}
	}

	close(auxPipeFileDescriptors[0]);
	close(auxPipeFileDescriptors[1]);
}
