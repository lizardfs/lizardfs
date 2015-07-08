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

const uint kBlockSize = 64 * 1024;

/*
 * Function saves a block of data in filestream and calculates its CRC saved in bytes buffer
 */
void saveBlockAndCrc(std::vector<char>& bufferParityCrcs, std::vector<char>& bufferParityBlock,
		std::shared_ptr<std::ofstream> partParity) {
	uint32_t crc = crc32(0, reinterpret_cast<const Bytef*>(bufferParityBlock.data()),
			bufferParityBlock.size());

	bufferParityCrcs.push_back((crc >> 24) & 0xFF);
	bufferParityCrcs.push_back((crc >> 16) & 0xFF);
	bufferParityCrcs.push_back((crc >> 8) & 0xFF);
	bufferParityCrcs.push_back(crc & 0xFF);

	partParity->write(bufferParityBlock.data(), kBlockSize);
	bufferParityBlock = std::vector<char>(kBlockSize, 0);
}



int main(int argc, char **argv) {

	// Define constant values
	const uint kXorMinLevel = 2;
	const uint kXorMaxLevel = 10;
	const uint kCrcSize = 4;
	const uint kBlocksInChunk = 1024;
	const uint kHeaderSize = 1024;
	const uint kCrcBegin = 1024;
	const uint kBlocksBegin = 4 * 1024;
	const uint kChunkIdLengthInFilename = 16;
	const uint kChunkVersionLengthInFilename = 8;
	const uint kChunkExtensionLengthInFilename = 4;
	const uint kDashLengthInFilename = 1;
	const uint kChunkNameFooterLength = kDashLengthInFilename + kChunkIdLengthInFilename
			+ kDashLengthInFilename + kChunkVersionLengthInFilename
			+ kChunkExtensionLengthInFilename;

	enum ExitStatus {
		OK = 0,
		ERROR_BAD_CMD_ARGUMENTS,
		ERROR_BAD_XOR_LEVEL_VALUE,
		ERROR_BAD_CHUNK_FILENAME,
		ERROR_BAD_CHUNK_HEADER,
		ERROR_BAD_CHUNK_CRC,
		ERROR_BAD_CHUNK_BLOCK
	};
	const std::vector<char> kOldChunkSignature = { 'M', 'F', 'S', 'C', ' ', '1', '.', '0' };
	const std::vector<char> kNewChunkSignature = { 'L', 'I', 'Z', 'C', ' ', '1', '.', '0' };

	// Check input arguments
	if (argc != 4) {
		std::cerr << "Usage:\n\t" << argv[0] << " <xor_level> <input_file> <output_dir>"
				<< std::endl;
		return ERROR_BAD_CMD_ARGUMENTS;
	}

	uint8_t xorLevel = 0;
	try {
		xorLevel = std::stoi(std::string(argv[1]));
		if (xorLevel < kXorMinLevel || xorLevel > kXorMaxLevel) {
			throw ERROR_BAD_XOR_LEVEL_VALUE;
		}
	} catch (...) {
		std::cerr << "XOR level should be in range <" << std::to_string(kXorMinLevel) << ", "
				<< std::to_string(kXorMaxLevel) << ">" << std::endl;
		return ERROR_BAD_XOR_LEVEL_VALUE;
	}

	std::string inputFileName(argv[2]);
	std::string outputDirName(argv[3]);
	std::ifstream inputFile(inputFileName);
	if (inputFile.fail()) {
		std::cerr << "Input file '" << inputFileName << "' can't be opened!" << std::endl;
		return ERROR_BAD_CHUNK_FILENAME;
	}

	// Create destination filenames and file streams
	std::string nameFooter;
	try {
		nameFooter = inputFileName.substr(inputFileName.size() - kChunkNameFooterLength,
				kChunkNameFooterLength);
	} catch (...) {
		std::cerr << "Chunk filename '" << inputFileName << "' is incorrect" << std::endl;
		return ERROR_BAD_CHUNK_FILENAME;
	}

	std::vector<std::shared_ptr<std::ofstream>> parts;
	std::vector<std::string> partNames;

	for (uint8_t part = 0; part <= xorLevel; ++part) {
		std::string partFileName = outputDirName + "/chunk_xor_";
		if (part == xorLevel) {
			partFileName += "parity";
		} else {
			partFileName += std::to_string(part + 1);
		}

		partFileName += "_of_" + std::to_string(xorLevel) + nameFooter;

		if (partFileName == inputFileName) {
			std::cerr << "Trying to save chunk with the same name as source chunk!" << std::endl;
			return ERROR_BAD_CHUNK_FILENAME;
		}

		partNames.push_back(partFileName);
		parts.push_back(std::make_shared<std::ofstream>(partFileName));
		parts.back()->exceptions(std::ios::failbit);
	}

	const uint8_t kMaxPartIndex = xorLevel - 1;
	std::shared_ptr<std::ofstream> partParity = parts[xorLevel];

	// Copy and make changes in the header
	std::vector<char> buffer(kBlockSize);
	if (!inputFile.read(buffer.data(), kHeaderSize)) {
		std::cerr << "Can't read all header!" << std::endl;
		return ERROR_BAD_CHUNK_HEADER;
	}

	bool signatureIsValid = true;
	std::vector<std::vector<char>> signatures = {kNewChunkSignature, kOldChunkSignature};
	for (const std::vector<char>& signature : signatures) {
		signatureIsValid = true;
		for (size_t i = 0; i < signature.size(); ++i) {
			if (signature[i] != buffer[i]) {
				signatureIsValid = false;
			}
		}
		if (signatureIsValid) {
			break;
		}
	}
	if (!signatureIsValid) {
		std::cerr << "Incorrect chunk file signature!" << std::endl;
		return ERROR_BAD_CHUNK_HEADER;
	}

	for (size_t part = 0; part <= xorLevel; ++part) {
		std::copy(kNewChunkSignature.begin(), kNewChunkSignature.end(), buffer.begin());

		if (part == xorLevel) {
			buffer[20] = (kXorMaxLevel + 1) * xorLevel;
		} else {
			buffer[20] = (kXorMaxLevel + 1) * xorLevel + part + 1; // from ChunkType::getXorChunkType()
		}

		parts[part]->write(buffer.data(), kHeaderSize);
	}

	// Dispense parts CRCs
	for (uint i = 0; i < kBlocksInChunk; ++i) {
		if (!inputFile.read(buffer.data(), kCrcSize)) {
			std::cerr << "Can't read all checksums!" << std::endl;
			return ERROR_BAD_CHUNK_CRC;
		}

		parts[i % xorLevel]->write(buffer.data(), kCrcSize);
	}

	// Move in parts to blocks begin
	for (size_t part = 0; part <= xorLevel; ++part) {
		parts[part]->seekp(kBlocksBegin - 1);
		parts[part]->put('\0'); // force zero-padding
	}
	partParity->seekp(kBlocksBegin);

	// Dispense blocks
	uint blockCount = 0;
	std::vector<char> bufferParityCrcs;
	std::vector<char> bufferParityBlock(kBlockSize, 0);
	size_t currentPart = 0;

	while (inputFile.read(buffer.data(), kBlockSize)) {
		currentPart = blockCount++ % xorLevel;

		// Calculating XOR
		for (uint i = 0; i < kBlockSize; ++i) {
			bufferParityBlock[i] ^= buffer[i];
		}

		parts[currentPart]->write(buffer.data(), kBlockSize);
		if (blockCount > kBlocksInChunk) {
			std::cerr << "Too much blocks in chunk!" << std::endl;
			return ERROR_BAD_CHUNK_BLOCK;
		}

		// Saving XOR in file
		if (currentPart == kMaxPartIndex) {
			saveBlockAndCrc(bufferParityCrcs, bufferParityBlock, partParity);
		}
	}

	// When division isn't equal
	if (blockCount && currentPart != kMaxPartIndex) {
		saveBlockAndCrc(bufferParityCrcs, bufferParityBlock, partParity);
	}

	// Save CRCs
	partParity->seekp(kCrcBegin);
	partParity->write(bufferParityCrcs.data(), bufferParityCrcs.size());

	// Check state after work
	if (inputFile.gcount()) {
		std::cerr << "Some data lasted in source chunk! Is chunk corrupted?" << std::endl;
		return ERROR_BAD_CHUNK_BLOCK;
	}

	std::cout << "Chunk '" << inputFileName << "' converted successfully to:" << std::endl;
	for (std::string partFileName : partNames) {
		std::cout << '\t' << partFileName << std::endl;
	}
	return OK;
}
