#pragma once
#include "common/platform.h"

#include <atomic>
#include <memory>
#include <string>

/**
 * A class which provides an interface for modifying different registered variables.
 */
class Tweaks {
public:
	Tweaks();
	~Tweaks();

	/// Adds a new bool variable.
	void registerVariable(const std::string& name, std::atomic<bool>& variable);

	/// Adds a new uint32_t variable.
	void registerVariable(const std::string& name, std::atomic<uint32_t>& variable);

	/// Adds a new uint64_t variable.
	void registerVariable(const std::string& name, std::atomic<uint64_t>& variable);

	/// Changes value of all variables with the given name.
	void setValue(const std::string& name, const std::string& value);

	/// Returns values of all the registered variables.
	std::string getAllValues() const;

private:
	class Impl;
	std::unique_ptr<Impl> impl_;
};

extern Tweaks gTweaks;
