#pragma once
#include "common/platform.h"

#include <istream>
#include <map>
#include <string>

class IoLimitsConfigLoader {
public:
	typedef std::map<std::string, uint64_t> LimitsMap;

	void load(std::istream&& stream);
	const std::string& subsystem() const { return subsystem_; }
	const LimitsMap& limits() const { return limits_; }

private:
	LimitsMap limits_;
	std::string subsystem_;
};
