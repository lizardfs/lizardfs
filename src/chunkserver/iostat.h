/*
   Copyright 2017 Skytechnology sp. z o.o..

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.

   Based on sysstat's
   iostat: report CPU and I/O statistics
   (C) 1998-2016 by Sebastien GODARD (sysstat <at> orange.fr)
 */

#include "common/platform.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <unordered_map>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#ifndef __linux__

class IoStat {
public:
	void resetPaths(const std::vector<std::string> &) {}
	uint8_t getLoadFactor() { return 0; }
};

#else
#include <cstdio>
#include <ctime>
#include <sys/vfs.h>

class IoStat {
protected:
	static const int kMaxLineLen = 1024;
	static const int kMaxNameLen = 1024;
	static const int kMaxPathLen = 4096;
	static const int kVariableCount = 14;
	static const constexpr char *kDiskstats = "/proc/diskstats";

	struct StatEntry {
		unsigned ticks;
		size_t size;
	};

	typedef std::unordered_map<dev_t, StatEntry> StatsMap;

public:
	void resetPaths(const std::vector<std::string> &paths) {
		prev_timestamp_ = 0;
		prev_load_factor_ = 0;
		for (auto &path : paths) {
			struct stat st;
			struct statfs stfs;
			int ret = ::stat(path.c_str(), &st);
			if (ret < 0) {
				// stat failed, ignore this directory
				continue;
			}
			ret = ::statfs(path.c_str(), &stfs);
			if (ret < 0) {
				continue;
			}
			stats_[st.st_dev] = {0, stfs.f_blocks};
		}
	}

	uint8_t getLoadFactor() {
		std::FILE *fp;
		char line[kMaxLineLen], dev_name[kMaxNameLen];
		int i;
		unsigned int ios_pgr, tot_ticks, rq_ticks, wr_ticks;
		unsigned long rd_ios, rd_merges_or_rd_sec, rd_ticks_or_wr_sec, wr_ios;
		unsigned long wr_merges, rd_sec_or_wr_ios, wr_sec;
		unsigned int major, minor;

		if (stats_.size() == 0) {
			return 0;
		}
		if (prev_timestamp_ >= (unsigned long)std::time(nullptr)) {
			return prev_load_factor_;
		}
		if ((fp = fopen(kDiskstats, "r")) == nullptr) {
			return 0;
		}

		unsigned long long sum = 0;
		size_t total_size = 0;
		while (fgets(line, sizeof(line), fp) != nullptr) {
			// major minor name rio rmerge rsect ruse wio wmerge wsect wuse running use aveq
			i = sscanf(line, "%u %u %s %lu %lu %lu %lu %lu %lu %lu %u %u %u %u",
				&major, &minor, dev_name,
				&rd_ios, &rd_merges_or_rd_sec, &rd_sec_or_wr_ios, &rd_ticks_or_wr_sec,
				&wr_ios, &wr_merges, &wr_sec, &wr_ticks, &ios_pgr, &tot_ticks, &rq_ticks);

			auto it = stats_.find(makedev(major, minor));
			if (i == kVariableCount && it != stats_.end()) {
				StatEntry &entry = it->second;
				sum += (tot_ticks - entry.ticks) * entry.size;
				total_size += entry.size;
				entry.ticks = tot_ticks;
			}
		}

		unsigned long timestamp = (unsigned long)std::time(nullptr);
		unsigned long timestamp_diff = timestamp - prev_timestamp_;
		// mean of milliseconds spent on I/O divided by time difference in seconds
		// and divided again by 10 will give a mean percentage of I/O load for all used paths
		unsigned long divisor = total_size * timestamp_diff * 10;
		// sometimes factor can be greater than 100 because timestamp is set _after_
		// reading disk stats (better to overestimate a bit than underestimate).
		prev_load_factor_ = std::min(sum / (divisor ? divisor : 1), 100ULL);
		prev_timestamp_ = timestamp;
		fclose(fp);
		return prev_load_factor_;
	}

protected:
	unsigned long prev_timestamp_;
	unsigned long prev_load_factor_;
	StatsMap stats_;
};

#endif
