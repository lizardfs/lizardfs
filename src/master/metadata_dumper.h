#pragma once

#include <errno.h>
#include <poll.h>
#include <string>
#include <syslog.h>
#include <unistd.h>

#include "common/slogger.h"

class MetadataDumper {
public:
	enum DumpType {
		kForegroundDump,
		kBackgroundDump
	};

	MetadataDumper(
			const std::string& metadataBackFilename,
			const std::string& metadataTmpBackFilename);

	bool dumpSucceeded() const;
	bool inProgress() const;
	bool useMetarestore() const;

	void setMetarestorePath(const std::string& path);
	void setUseMetarestore(bool val);

	/// returns true and modifies dumpType (to FOREGROUND_DUMP) if we return as a child
	bool start(DumpType& dumpType, uint64_t checksum);

	// for poll
	void pollDesc(struct pollfd *pdesc, uint32_t *ndesc);
	void pollServe(struct pollfd *pdesc);

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
	std::string metadataBackFilename_;
	std::string metadataTmpBackFilename_;
};
