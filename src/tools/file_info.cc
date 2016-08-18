/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2017 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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

#include <stdio.h>

#include "common/chunk_copies_calculator.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static void file_info_usage() {
	fprintf(stderr,
	        "show files info (shows detailed info of each file chunk)\n\nusage:\n"
	        " lizardfs fileinfo name [name ...]\n");
}

static std::string chunkTypeToString(ChunkPartType type) {
	if (slice_traits::isXor(type) || slice_traits::isEC(type)) {
		std::stringstream ss;
		ss << " part " << type.getSlicePart() + 1 << "/" << slice_traits::getNumberOfParts(type)
		   << " of " << to_string(type.getSliceType());
		return ss.str();
	}
	return "";
}

static int file_info(const char *fileName) {
	std::vector<uint8_t> buffer;
	uint32_t chunkIndex, inode, chunkVersion, messageId = 0;
	uint64_t fileLength, chunkId;
	int fd;
	fd = open_master_conn(fileName, &inode, nullptr, 0, 0);
	if (fd < 0) {
		return -1;
	}
	chunkIndex = 0;
	try {
		buffer.clear();
		cltoma::tapeInfo::serialize(buffer, 0, inode);
		if (tcpwrite(fd, buffer.data(), buffer.size()) != (int)buffer.size()) {
			printf("%s [tape info]: master: send error\n", fileName);
			close_master_conn(1);
			return -1;
		}

		buffer.resize(PacketHeader::kSize);
		if (tcpread(fd, buffer.data(), PacketHeader::kSize) != (int)PacketHeader::kSize) {
			printf("%s [tape info]: master query: receive error\n", fileName);
			close_master_conn(1);
			return -1;
		}

		PacketHeader header;
		deserializePacketHeader(buffer, header);

		if (header.type != LIZ_MATOCL_TAPE_INFO) {
			printf("%s [tape info]: master query: wrong answer (type)\n", fileName);
			close_master_conn(1);
			return -1;
		}

		buffer.resize(header.length);

		if (tcpread(fd, buffer.data(), header.length) != (int)header.length) {
			printf("%s [tape info]: master query: receive error\n", fileName);
			close_master_conn(1);
			return -1;
		}

		PacketVersion version;
		deserialize(buffer, version, messageId);

		if (messageId != 0) {
			printf("%s [tape info]: master query: wrong answer (queryid)\n", fileName);
			close_master_conn(1);
			return -1;
		}

		uint8_t status = LIZARDFS_STATUS_OK;
		if (version == matocl::tapeInfo::kStatusPacketVersion) {
			matocl::tapeInfo::deserialize(buffer, messageId, status);
		} else if (version != matocl::tapeInfo::kResponsePacketVersion) {
			printf("%s [tape info]: master query: wrong answer (packet version)\n", fileName);
			close_master_conn(1);
			return -1;
		}
		if (status != LIZARDFS_STATUS_OK) {
			printf("%s [tape info]: %s\n", fileName, mfsstrerr(status));
			close_master_conn(1);
			return -1;
		}

		std::vector<TapeCopyLocationInfo> tapeCopies;
		matocl::tapeInfo::deserialize(buffer, messageId, tapeCopies);

		printf("%s:\n", fileName);
		if (tapeCopies.size() != 0) {
			for (unsigned i = 0; i < tapeCopies.size(); i++) {
				std::string copyStatus;
				switch (tapeCopies[i].state) {
				case TapeCopyState::kInvalid:
					copyStatus = "invalid";
					break;
				case TapeCopyState::kCreating:
					copyStatus = "creating";
					break;
				case TapeCopyState::kOk:
					copyStatus = "OK";
					break;
				}
				printf("\ttape replica %u: %s on %s (%s, %s)\n", i + 1, copyStatus.c_str(),
				       tapeCopies[i].tapeserver.server.c_str(),
				       tapeCopies[i].tapeserver.address.toString().c_str(),
				       tapeCopies[i].tapeserver.label.c_str());
			}
		}

		do {
			buffer.clear();
			cltoma::chunkInfo::serialize(buffer, 0, inode, chunkIndex);
			if (tcpwrite(fd, buffer.data(), buffer.size()) != (int)buffer.size()) {
				printf("%s [%" PRIu32 "]: master query: send error\n", fileName, chunkIndex);
				close_master_conn(1);
				return -1;
			}

			buffer.resize(PacketHeader::kSize);
			if (tcpread(fd, buffer.data(), PacketHeader::kSize) != (int)PacketHeader::kSize) {
				printf("%s [%" PRIu32 "]: master query: receive error\n", fileName, chunkIndex);
				close_master_conn(1);
				return -1;
			}

			PacketHeader header;
			deserializePacketHeader(buffer, header);

			if (header.type != LIZ_MATOCL_CHUNK_INFO) {
				printf("%s [%" PRIu32 "]: master query: wrong answer (type)\n", fileName,
				       chunkIndex);
				close_master_conn(1);
				return -1;
			}

			buffer.resize(header.length);

			if (tcpread(fd, buffer.data(), header.length) != (int)header.length) {
				printf("%s [%" PRIu32 "]: master query: receive error\n", fileName, chunkIndex);
				close_master_conn(1);
				return -1;
			}

			PacketVersion version;
			deserialize(buffer, version, messageId);

			if (messageId != 0) {
				printf("%s [%" PRIu32 "]: master query: wrong answer (queryid)\n", fileName,
				       chunkIndex);
				close_master_conn(1);
				return -1;
			}

			uint8_t status = LIZARDFS_STATUS_OK;
			if (version == matocl::chunkInfo::kStatusPacketVersion) {
				matocl::chunkInfo::deserialize(buffer, messageId, status);
			} else if (version != matocl::chunkInfo::kECChunks_ResponsePacketVersion) {
				printf("%s [%" PRIu32 "]: master query: wrong answer (packet version)\n", fileName,
				       chunkIndex);
				close_master_conn(1);
				return -1;
			}
			if (status != LIZARDFS_STATUS_OK) {
				printf("%s [%" PRIu32 "]: %s\n", fileName, chunkIndex, mfsstrerr(status));
				close_master_conn(1);
				return -1;
			}

			std::vector<ChunkWithAddressAndLabel> copies;
			matocl::chunkInfo::deserialize(buffer, messageId, fileLength, chunkId, chunkVersion,
			                               copies);

			if (fileLength > 0) {
				if (chunkId == 0 && chunkVersion == 0) {
					printf("\tchunk %" PRIu32 ": empty\n", chunkIndex);
				} else {
					printf("\tchunk %" PRIu32 ": %016" PRIX64 "_%08" PRIX32 ""
							" / (id:%" PRIu64 " ver:%" PRIu32 ")\n",
							chunkIndex, chunkId, chunkVersion, chunkId, chunkVersion);
					ChunkCopiesCalculator chunk_calculator;
					for(const auto &part : copies) {
						chunk_calculator.addPart(part.chunkType, MediaLabel::kWildcard);
					}
					chunk_calculator.evalRedundancyLevel();
					if (copies.size() > 0) {
						std::sort(copies.begin(), copies.end());
						for (size_t i = 0; i < copies.size(); i++) {
							printf("\t\tcopy %lu: %s:%s%s\n", i + 1,
									copies[i].address.toString().c_str(),
									copies[i].label.c_str(),
									chunkTypeToString(copies[i].chunkType).c_str());
						}
					}
					if (chunk_calculator.getFullCopiesCount() == 0) {
						if (copies.size() == 0) {
							printf("\t\tno valid copies !!!\n");
						} else {
							printf("\t\tnot enough parts available\n");
						}
					}
				}
			}
			chunkIndex++;
		} while (chunkIndex < ((fileLength + MFSCHUNKMASK) >> MFSCHUNKBITS));
	} catch (IncorrectDeserializationException &e) {
		printf("%s [%" PRIu32 "]: master query: wrong answer (%s)\n", fileName, chunkIndex,
		       e.what());
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	return 0;
}

int file_info_run(int argc, char **argv) {
	int status;

	while (getopt(argc, argv, "") != -1) {
	}
	argc -= optind;
	argv += optind;

	if (argc < 1) {
		file_info_usage();
		return 1;
	}

	status = 0;
	while (argc > 0) {
		if (file_info(*argv) < 0) {
			status = 1;
		}
		argc--;
		argv++;
	}
	return status;
}
