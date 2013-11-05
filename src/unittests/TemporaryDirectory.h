/*
 * TemporaryDirecory.h
 *
 *  Created on: 03-07-2013
 *      Author: Marcin Sulikowski
 */

#ifndef TEMPORARYDIRECORY_H_
#define TEMPORARYDIRECORY_H_

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

#endif /* TEMPORARYDIRECORY_H_ */
