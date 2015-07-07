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
