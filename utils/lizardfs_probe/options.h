#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/massert.h"

class Options {
public:
	LIZARDFS_CREATE_EXCEPTION_CLASS(ParseError, Exception);

	Options(const std::vector<std::string>& expectedArgs, const std::vector<std::string>& argv);

	const std::vector<std::string>& arguments() const {
		return arguments_;
	}

	const std::string& arguments(uint32_t pos) const {
		return arguments_[pos];
	}

	bool isSet(const std::string& name) const {
		sassert(isOptionExpected(name));
		return options_.at(name);
	}

	bool isOptionExpected(const std::string& name) const {
		return options_.count(name) > 0;
	}

private:
	std::map<std::string, bool> options_;
	std::vector<std::string> arguments_;
};
