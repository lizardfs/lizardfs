/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#include "common/platform.h"

#include <errno.h>
#include <poll.h>
#include <syslog.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "common/slogger.h"
#include "common/time_utils.h"

class MetadataDumper {
public:
	enum DumpType {
		kForegroundDump,
		kBackgroundDump
	};

	MetadataDumper(
			const std::string& metadataFilename,
			const std::string& metadataTmpFilename);

	bool dumpSucceeded() const;
	bool inProgress() const;
	bool useMetarestore() const;

	void setMetarestorePath(const std::string& path);
	void setUseMetarestore(bool val);

	/// returns true and modifies dumpType (to FOREGROUND_DUMP) if we return as a child
	bool start(DumpType& dumpType, uint64_t checksum);

	// for poll
	void pollDesc(std::vector<pollfd> &pdesc);
	void pollServe(const std::vector<pollfd> &pdesc);

	/// waits until the metadumper finishes
	void waitUntilFinished();

	/// waits until the metadumper finishes but not longer than timeout
	void waitUntilFinished(SteadyDuration timeout);

protected:
	void dumpingFinished();

	/// how long can the decimal representation of a(n) (u)int64 be
	static const uint32_t kInt64MaxDecimalLength = 21;

	/// should the metarestore be used at all
	bool useMetarestore_;

	/// if last dump was unsuccessful, now dump by master
	bool dumpingSucceeded_;

	/// fd of the reading end of the pipe (connected to stdout of the dumping process)
	int dumpingProcessFd_;

	/// pos in `pollfd`s array
	int32_t dumpingProcessPollFdsPos_;

	/// the dumping process has written something
	bool dumpingProcessOutputEmpty_;

	std::string metarestorePath_;
	std::string metadataFilename_;
	std::string metadataTmpFilename_;
};
