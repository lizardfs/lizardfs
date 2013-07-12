// I know it's sad, just dont have time to change poorly testable code right now
#include "mfschunkserver/hddspacemgr.cc"

#include <cstdlib>
#include <string>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>
#include <gtest/gtest.h>

#include "tests/common/TemporaryDirectory.h"

class HddSpaceManager : public testing::Test {
public:
	static void SetUpTestCase() {
		mycrc32_init();
	}
};

TEST_F(HddSpaceManager, hddReadTest) {
	folder f;
	folder* fptr = &f;

	std::string chunkserverParentDir = "/tmp"; // TODO change to a parameter with default value
	TemporaryDirectory temp(chunkserverParentDir, "hddFolderScanTest");
	std::string chunkserverDir = temp.name() + "/";

	f.path = (char*)chunkserverDir.c_str();
	f.needrefresh = 1;
	f.lasterrindx = 0;
	f.lastrefresh = 0;
	f.needrefresh = 1;
	f.scanstate = SCST_SCANNEEDED;
	f.testhead = NULL;
	f.testtail = &(f.testhead);
	f.next = folderhead;
	folderhead = &f;

	// FIXME a fragment of hdd_init code, to be redesigned
	for (uint32_t hp = 0 ; hp < HASHSIZE; hp++) {
		hashtab[hp] = NULL;
	}
	for (uint32_t hp = 0 ; hp < DHASHSIZE; hp++) {
		dophashtab[hp] = NULL; // TODO zrozumiec coto
	}

	{
		SCOPED_TRACE("Creating empty chunk");
		hdd_folder_scan((void*) fptr);
		ASSERT_EQ(SCST_SCANFINISHED, f.scanstate);
		f.scanstate = SCST_WORKING;
		ASSERT_EQ(STATUS_OK, hdd_int_create(10, 1));
		ASSERT_EQ(STATUS_OK, hdd_open(10));
	}

	const int blockCount = std::min(MFSBLOCKSINCHUNK, 5);

	{
		SCOPED_TRACE("Writing chunk block by block");
		boost::scoped_array<uint8_t> buf(new uint8_t[MFSBLOCKSIZE]);
		for (int i = 0; i < blockCount; ++i) {
			SCOPED_TRACE("Writing block " + boost::lexical_cast<std::string>(i));
			memset(buf.get(), i, MFSBLOCKSIZE);
			uint32_t crc = mycrc32(0, buf.get(), MFSBLOCKSIZE);
			const uint8_t* tmpPtr = (uint8_t*)&crc;
			crc = get32bit(&tmpPtr);
			ASSERT_EQ(STATUS_OK, hdd_write(10, 1, i, buf.get(), 0, MFSBLOCKSIZE, (const uint8_t*)&crc));
		}
	}


	{
		SCOPED_TRACE("Reading chunk block by block");
		uint32_t crc;
		boost::scoped_array<uint8_t> buf(new uint8_t[MFSBLOCKSIZE]);
		for (int i = 0; i < blockCount; ++i) {
			SCOPED_TRACE("Reading block " + boost::lexical_cast<std::string>(i));
			ASSERT_EQ(STATUS_OK, hdd_read(10, 1, buf.get(), MFSBLOCKSIZE * i, MFSBLOCKSIZE, (uint8_t*)&crc));
			for (int j = 0; j < MFSBLOCKSIZE; ++j) {
				ASSERT_EQ(i, buf[j]) << "Byte " << j << " in block doesn't match";
			}
		}
	}

	{
		SCOPED_TRACE("Reading whole chunk in one hdd_read");
		uint32_t crc;
		boost::scoped_array<uint8_t> largeBuf(new uint8_t[MFSBLOCKSIZE * blockCount]);
		ASSERT_EQ(STATUS_OK, hdd_read(10, 1, largeBuf.get(), 0, MFSBLOCKSIZE * blockCount, (uint8_t*)&crc));
		for (int i = 0; i < blockCount; ++i) {
			SCOPED_TRACE("Testing block " + boost::lexical_cast<std::string>(i));
			for (int j = 0; j < MFSBLOCKSIZE; ++j) {
				ASSERT_EQ(i, largeBuf[i * MFSBLOCKSIZE + j]) << "Byte " << j << " in block doesn't match";
			}
		}
	}
}
