#pragma once

#include <map>
#include <string>
#include <istream>

#include "common/exception.h"

class IoLimitsConfigLoader {
public:
	typedef std::map<std::string, uint64_t> LimitsMap;

	LIZARDFS_CREATE_EXCEPTION_CLASS(ParseException, Exception);

	void load(std::istream&& stream);
	const std::string& subsystem() const { return subsystem_; }
	const LimitsMap& limits() const { return limits_; }

private:
	LimitsMap limits_;
	std::string subsystem_;
};
