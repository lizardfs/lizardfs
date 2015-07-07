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

#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>
#include <vector>
#include <zlib.h>
#include <vector>
#include <regex>
#include <arpa/inet.h>

const uint32_t kCrcBytesCount = 4;
const uint32_t kBlockSize = 64 * 1024;
const uint32_t kCrcOffset = 1024;

enum ExitStatus {
	ERROR_BAD_CMD_ARGUMENTS = 1,
	ERROR_INCORECT_CHUNK_FILE,
	ERROR_READ_FILE
};

class ChunkType {
public:
	enum Type {
		kStandard,
		kXor
	};

	ChunkType(Type type = kStandard) {
		type_ = type;
		switch (type) {
		case kStandard:
			dataOffset_ = 5 * 1024;
			maxBlockCount_ = 1024;
			crcDataSize_ = 4 * 1024;
			break;
		case kXor:
			dataOffset_ = 4 * 1024;
			maxBlockCount_ = 512;
			crcDataSize_ = 4 * 512;
			break;
		}
	}

	Type type() const {
		return type_;
	}

	uint32_t dataOffset() const {
		return dataOffset_;
	}

	uint32_t maxBlockCount() const {
		return maxBlockCount_;
	}

	uint32_t crcDataSzie() const {
		return crcDataSize_;
	}

private:
	Type type_;
	uint32_t dataOffset_;
	uint32_t maxBlockCount_;
	uint32_t crcDataSize_;
};

void usage(const char* name) {
	std::cerr << "Usage:\n\t" << name << " <input_file> { Std | Xor }"
			<< std::endl;
}

void parseArguments(std::string& inputFile, ChunkType& chunkType, int argc, char **argv);
void calculateCrc(uint8_t* crcDestination, const std::string& inputChunkFile,
		const ChunkType& chunkType);
bool calculateCrcForBlock(uint32_t& crc, std::ifstream& stream);
void writeCrc(const uint8_t* crcData, const std::string& fileName, const ChunkType& chunkType);

int main(int argc, char **argv) {
	std::string inputFile;
	ChunkType chunkType;
	parseArguments(inputFile, chunkType, argc, argv);

	std::vector<uint8_t> crc(chunkType.maxBlockCount() * kCrcBytesCount);
	calculateCrc(crc.data(), inputFile, chunkType);
	writeCrc(crc.data(), inputFile, chunkType);
}

void calculateCrc(uint8_t* crcDestination, const std::string& inputChunkFile,
		const ChunkType& chunkType) {
	std::ifstream chunkFileStream(inputChunkFile, std::ifstream::in);
	chunkFileStream.seekg(chunkType.dataOffset());

	while (!chunkFileStream.eof()) {
		uint32_t crc;
		if (calculateCrcForBlock(crc, chunkFileStream) == false) {
			if (!chunkFileStream.eof()) {
				exit(ERROR_READ_FILE);
			}
			break;
		}
		*reinterpret_cast<uint32_t*>(crcDestination) = htonl(crc);
		crcDestination += 4;
	}

	chunkFileStream.close();
}

bool calculateCrcForBlock(uint32_t& crc, std::ifstream& stream) {
	std::vector<char> blockBuffer(kBlockSize);
	stream.read(blockBuffer.data(), kBlockSize);
	if (stream.gcount() == 0) {
		return false;
	} else if (stream.gcount() < kBlockSize) {
		exit(ERROR_INCORECT_CHUNK_FILE);
	}
	crc = crc32(0, reinterpret_cast<uint8_t*>(blockBuffer.data()), kBlockSize);
	return true;
}

void writeCrc(const uint8_t* crcData, const std::string& fileName, const ChunkType& chunkType) {
	std::fstream stream(fileName, std::fstream::in | std::fstream::out);
	stream.seekg(kCrcOffset);
	stream.write(reinterpret_cast<const char*>(crcData), chunkType.crcDataSzie());
	stream.close();
}

void parseArguments(std::string& inputFile, ChunkType& chunkType, int argc, char** argv) {
	if (argc != 3) {
		usage(argv[0]);
		exit(ERROR_BAD_CMD_ARGUMENTS);
	}

	inputFile = std::string(argv[1]);
	std::string chunkTypeString = std::string(argv[2]);

	if (chunkTypeString == std::string("Std")) {
		chunkType = ChunkType(ChunkType::kStandard);
	} else if (chunkTypeString == std::string("Xor")) {
		chunkType = ChunkType(ChunkType::kXor);
	} else {
		usage(argv[0]);
		exit(ERROR_BAD_CMD_ARGUMENTS);
	}
}
