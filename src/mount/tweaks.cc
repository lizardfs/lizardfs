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

	std::string getValue() const {
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
