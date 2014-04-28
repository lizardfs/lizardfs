#pragma once

#include "config.h"

#include <exception>
#include <string>

#include "common/MFSCommunication.h"
#include "common/mfsstrerr.h"

class Exception : public std::exception {
public:
	Exception(const std::string& message) : message_(message), status_(STATUS_OK) {
	}

	Exception(const std::string& message, uint8_t status) : message_(message), status_(status) {
		if (status != STATUS_OK) {
			message_ += " (" + std::string(mfsstrerr(status)) + ")";
		}
	}

	~Exception() throw() {}

	const char* what() const throw() {
		return message_.c_str();
	}

	const std::string& message() const throw() {
		return message_;
	}

	uint8_t status() const throw() {
		return status_;
	}

private:
	std::string message_;
	uint8_t status_;
};

#define LIZARDFS_CREATE_EXCEPTION_CLASS(name, base) \
	class name : public base { \
	public: \
		name(const std::string& message) : base(message) {} \
		name(const std::string& message, uint8_t status) : base(message, status) {} \
		~name() throw() {} \
	}

#define LIZARDFS_CREATE_EXCEPTION_CLASS_MSG(name, base, message) \
	class name : public base { \
	public: \
		name() : base(std::string(message)) {} \
		name(uint8_t status) : base(std::string(message), status) {} \
		~name() throw() {} \
	}
