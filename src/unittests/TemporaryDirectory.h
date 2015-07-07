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

/*
 * TemporaryDirecory.h
 *
 *  Created on: 03-07-2013
 *      Author: Marcin Sulikowski
 */

#pragma once

#include "common/platform.h"

#include <sys/time.h>
#include <unistd.h>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>

/**
 * Class holding a temporary directory
 */
class TemporaryDirectory {
private:
	std::string name_;
public:
	TemporaryDirectory(const std::string& prefix, const std::string& comment = "") {
		if (comment.find('/') != comment.npos) {
			throw new std::runtime_error("Wrong TemporaryDirectory comment: '" + comment + "'; cannot contain slash");
		}

		// Create a name that will never be the same (on one machine)
		// It also contains a timestamp, so we know when it was created if it will not be removed
		struct timeval tv;
		gettimeofday(&tv, NULL);
		name_ = boost::str(boost::format("%1%/temp_%2%.%3%_%4%") % prefix % tv.tv_sec % tv.tv_usec % getpid());

		if (!comment.empty()) {
			name_ += "_" + comment;
		}
		boost::filesystem::create_directory(name_);
	}

	/**
	 * Destructor removes the temporary directory recursively
	 */
	~TemporaryDirectory() {
		boost::system::error_code ec;
		boost::filesystem::remove_all(name_, ec);
	}

	const std::string& name() const {
		return name_;
	}
};
