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

#pragma once
#include "config.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "common/portable_endian.h"

#include "utils/asserts.h"
#include "utils/configuration.h"

/**
 * This class generates files in the following format:
 * - first 8 bytes: size of the file (thus the minimal size is 8 bytes)
 * - then a sequence of 8-byte blocks, each block contains a value
 *     if seed == 0 => (offset + 0x0807060504030201) % 2^64
 *     else         => (offset + (rand << 32 | rand)) % 2^64
 * If the file size does not divide by 8 the last block is truncated.
 * All numbers (uint64) are in big endian format
 * (it is easier for a human to read the hexdump -C of such file)
 */
class DataGenerator {
public:
	/**
	 * \param seed seed to initiate pseudorandom generation of the data pattern
	 */
	explicit DataGenerator(int seed = 0) noexcept : seed_(seed) {}

	void createFile(const std::string& name, uint64_t size) const {
		int fd = open(name.c_str(), O_WRONLY | O_CREAT | O_TRUNC, (mode_t)0644);
		utils_passert(fd >= 0);
		fillFileWithProperData(fd, size);
		utils_zassert(close(fd));
	}

	void overwriteFile(const std::string& name) const {
		struct stat fileInformation;
		utils_zassert(stat(name.c_str(), &fileInformation));
		int fd = open(name.c_str(), O_WRONLY, (mode_t)0644);
		utils_passert(fd >= 0);
		fillFileWithProperData(fd, fileInformation.st_size);
		utils_zassert(close(fd));
	}

	void validateFile(int fd, const std::string &name, uint64_t file_size = 0) const {
		std::string error;

		struct stat file_information;
		utils_zassert(stat(name.c_str(), &file_information));

		if (file_size == 0) {
			/* Check the file size */
			file_size = file_information.st_size;
			uint64_t expected_size;

			if(read(fd, &expected_size, sizeof(expected_size)) != sizeof(expected_size)) {
				// The file if too short, so the first bytes are corrupted.
				throw std::length_error("(inode " + std::to_string(file_information.st_ino) + ")"
						" file too short (" + std::to_string(file_size) + " bytes)");
			}
			expected_size = be64toh(expected_size);
			if (expected_size != (uint64_t)file_information.st_size) {
				error = "(inode " + std::to_string(file_information.st_ino) + ")"
						" file should be " + std::to_string(expected_size) +
						" bytes long, but is " + std::to_string(file_size) + " bytes long\n";
			}
		}

		/* Check the data */
		off_t current_offset = sizeof(uint64_t);
		uint64_t size = file_size - sizeof(uint64_t);
		std::vector<char> actual_buffer(UtilsConfiguration::blockSize());
		std::vector<char> proper_buffer(UtilsConfiguration::blockSize());
		while (size > 0) {
			uint64_t bytes_to_read = size;
			if (bytes_to_read > proper_buffer.size()) {
				bytes_to_read = proper_buffer.size();
			}
			proper_buffer.resize(bytes_to_read);
			fillBufferWithProperData(proper_buffer, current_offset);
			ssize_t bytes_read = pread(fd, actual_buffer.data(), bytes_to_read, current_offset);
			utils_passert((file_size == 0 && bytes_read == (ssize_t)bytes_to_read) || (file_size > 0 && bytes_read >= 0));
			size -= bytes_read;
			// memcmp is very fast, use it to check if everything is OK
			if (memcmp(actual_buffer.data(), proper_buffer.data(), bytes_read) == 0) {
				current_offset += bytes_read;
				continue;
			}
			// if not -- find the byte which is corrupted
			for (size_t i = 0; i < (size_t)bytes_read; ++i) {
				if (actual_buffer[i] != proper_buffer[i]) {
					std::stringstream ss;
					ss << "(inode " << file_information.st_ino << ")"
							<< " data mismatch at offset " << i << ", seed " << seed_ << ". Expected/actual:\n";
					for (size_t j = i; j < (size_t)bytes_read && j < i + 32; ++j) {
						ss << std::hex << std::setfill('0') << std::setw(2)
								<< static_cast<int>(static_cast<unsigned char>(proper_buffer[j]))
								<< " ";
					}
					ss << "\n";
					for (size_t j = i; j < (size_t)bytes_read && j < i + 32; ++j) {
						ss << std::hex << std::setfill('0') << std::setw(2)
								<< static_cast<int>(static_cast<unsigned char>(actual_buffer[j]))
								<< " ";
					}
					throw std::invalid_argument(error + ss.str());
				}
			}
			utils_mabort("memcmp returned non-zero, but there is no difference");
		}
		if (!error.empty()) {
			throw std::length_error(error + "The rest of the file is OK");
		}
	}

	/*
	 * This function checks if the file contains proper data
	 * generated by DataStream::createFile and throws an std::exception
	 * in case of the data is corrupted. If 'repeat_after' is set, validation
	 * should be repeated after given time (for testing various caches).
	 */
	void validateFile(const std::string& name, size_t repeat_after_ms) const {
		int fd = open(name.c_str(), O_RDONLY);
		utils_passert(fd != -1);
		if (repeat_after_ms > 0) {
			try {
				validateFile(fd, name);
			} catch (...) {
			}
			usleep(1000 * repeat_after_ms);
			lseek(fd, 0, SEEK_SET);
			validateFile(fd, name);
		} else {
			validateFile(fd, name);
		}
		utils_zassert(close(fd));
	}

	void validateGrowingFile(const std::string& name, size_t file_size) const {
		int fd = open(name.c_str(), O_RDONLY);
		utils_passert(fd != -1);
		validateFile(fd, name, file_size);
		utils_zassert(close(fd));
	}

protected:
	void fillBufferWithProperData(std::vector<char>& buffer, off_t offset) const {
		size_t size = buffer.size();
		if (offset % 8 == 0 && size % 8 == 0) {
			fillAlignedBufferWithProperData(buffer, offset);
			return;
		}
		/*
		 * If the buffer or offset is not aligned
		 * we will create aligned buffer big enough to
		 * be a superset of the buf, fill it with data
		 * and copy the proper part of it
		 */
		off_t alignedOffset = (offset / 8) * 8;
		std::vector<char> alignedBuffer(16 + (size / 8) * 8);
		fillAlignedBufferWithProperData(alignedBuffer, alignedOffset);
		memcpy(buffer.data(), alignedBuffer.data() + (offset - alignedOffset), size);
	}

	/**
	 * This function requires both offset and size to be multiples of 8
	 *
	 * Not thread-safe (as it uses std::rand).
	 */
	void fillAlignedBufferWithProperData(std::vector<char>& buffer, off_t offset) const {
		using BlockType = uint64_t;
		utils_massert(sizeof(BlockType) == 8);
		utils_massert(offset % sizeof(BlockType) == 0);
		utils_massert(buffer.size() % sizeof(BlockType) == 0);
		BlockType *blocks = (BlockType*)buffer.data();
		BlockType dataPattern = 0x0807060504030201ULL;
		if (seed_ != 0) {
			static_assert(
				sizeof(decltype(std::rand())) == sizeof(int32_t),
				"This implementation requires 32bit-sized return type of std::rand but its size on your platform is different"
			);
			std::srand(seed_);
			dataPattern = static_cast<BlockType>(std::rand()) << 32 | static_cast<uint32_t>(std::rand());
		}
		for (size_t i = 0; i < buffer.size() / sizeof(BlockType); ++i) {
			BlockType block = htobe64(dataPattern + offset);
			blocks[i] = block;
			offset += sizeof(BlockType);
		}
	}

	void fillFileWithProperData(int fd, uint64_t size) const {
		utils_massert(fd >= 0);

		/* Write the size of the file */
		uint64_t serializedSize = htobe64(size);
		utils_massert(size >= sizeof(serializedSize));
		utils_passert(write(fd, &serializedSize, sizeof(serializedSize))== sizeof(serializedSize));

		/* Write the data */
		size -= sizeof(serializedSize);
		off_t currentOffset = sizeof(serializedSize);
		std::vector<char> buffer(UtilsConfiguration::blockSize());
		while (size > 0) {
			size_t bytesToWrite = size;
			if (bytesToWrite > buffer.size()) {
				bytesToWrite = buffer.size();
			}
			buffer.resize(bytesToWrite);
			fillBufferWithProperData(buffer, currentOffset);
			utils_passert(write(fd, buffer.data(), bytesToWrite) == (ssize_t)bytesToWrite);
			size -= bytesToWrite;
			currentOffset += bytesToWrite;
		}
	}

private:
	int seed_;
};
