#pragma once

#include "common/platform.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>

/*! \brief String class keeping an additional field - precomputed hash.
 *
 *  Hash field is kept in order to speed up String comparison,
 *  which is a dominating operation in lookup (which is, recursively,
 *  dominating operation in file system).
 */
class HString : public ::std::string {
public:
	typedef uint32_t HashType;

	HString() : ::std::string() {
		computeHash();
	}

	explicit HString(const char *str) : ::std::string(str) {
		computeHash();
	}

	explicit HString(const ::std::string &str) : ::std::string(str) {
		computeHash();
	}

	explicit HString(const char* str, std::size_t length) : ::std::string(str, length) {
		computeHash();
	}

	template<class InputIterator>
	explicit HString(InputIterator first, InputIterator last) : ::std::string(first, last) {
		computeHash();
	}

	HString(const HString &str) : ::std::string(str) {
		computeHash();
	}

	HString(HString &&str) noexcept : ::std::string(std::move(str)) {
		computeHash();
	}

	HString &operator=(const HString &str) {
		::std::string::operator=(str);
		computeHash();
		return *this;
	}

	HString &operator=(HString &&str) noexcept {
		::std::string::operator=(std::move(str));
		computeHash();
		return *this;
	}

	HString &operator=(const ::std::string &str) {
		::std::string::operator=(str);
		computeHash();
		return *this;
	}

	HashType hash() const {
		return hash_;
	}

private:
	void computeHash() {
		hash_ = ::std::hash< ::std::string>()(*static_cast< ::std::string *>(this));
	}

	HashType hash_;
};

inline bool operator==(const HString &str, const HString &other) {
	if (str.hash() == other.hash()) {
		return static_cast< ::std::string>(str) == static_cast< ::std::string>(other);
	}
	return false;
}

inline bool operator!=(const HString &str, const HString &other) {
	if (str.hash() == other.hash()) {
		return static_cast< ::std::string>(str) != static_cast< ::std::string>(other);
	}
	return true;
}
