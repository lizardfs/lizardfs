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

#include "common/platform.h"
#include "mount/tweaks.h"

#include <list>
#include <sstream>

class Variable {
public:
	virtual ~Variable() {}
	virtual void setValue(const std::string& value) = 0;
	virtual std::string getValue() const = 0;
};

template <typename T>
class VariableImpl : public Variable {
public:
	VariableImpl(std::atomic<T>& v) : value_(&v) {}

	void setValue(const std::string& value) override {
		std::stringstream ss(value);
		T t;
		ss >> std::boolalpha >> t;
		if (!ss.fail()) {
			value_->store(t);
		}
	}

	std::string getValue() const override {
		std::stringstream ss;
		ss << std::boolalpha << value_->load();
		return ss.str();
	}

private:
	std::atomic<T>* value_;
};

template <typename T>
std::unique_ptr<Variable> makeVariable(std::atomic<T>& v) {
	return std::unique_ptr<Variable>(new VariableImpl<T>(v));
}

class Tweaks::Impl {
public:
	std::list<std::pair<std::string, std::unique_ptr<Variable>>> variables;
};

Tweaks::Tweaks() : impl_(new Impl) {}

Tweaks::~Tweaks() {}

void Tweaks::registerVariable(const std::string& name, std::atomic<bool>& variable) {
	impl_->variables.push_back({name, makeVariable(variable)});
}

void Tweaks::registerVariable(const std::string& name, std::atomic<uint32_t>& variable) {
	impl_->variables.push_back({name, makeVariable(variable)});
}

void Tweaks::registerVariable(const std::string& name, std::atomic<uint64_t>& variable) {
	impl_->variables.push_back({name, makeVariable(variable)});
}

void Tweaks::setValue(const std::string& name, const std::string& value) {
	for (auto& nameAndVariable : impl_->variables) {
		if (nameAndVariable.first == name) {
			nameAndVariable.second->setValue(value);
		}
	}
}

std::string Tweaks::getAllValues() const {
	std::stringstream ss;
	for (const auto& nameAndVariable : impl_->variables) {
		ss << nameAndVariable.first << "\t" << nameAndVariable.second->getValue() << "\n";
	}
	return ss.str();
}

Tweaks gTweaks;
