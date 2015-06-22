#pragma once

#include "common/platform.h"

#include "master/hstring.h"

namespace hstorage {
/*! \brief Class representing type of String storage
 *
 *  This class is used as a container for global String storage options.
 */

class Handle;

class Storage {
	template <int N>
	struct static_wrapper {
		static ::std::unique_ptr<Storage> instance_;
	};
public:
	virtual ~Storage() {
	}

	static Storage &instance() {
		return *static_wrapper<0>::instance_;
	}

	static void reset() {
		static_wrapper<0>::instance_.reset();
	}

	static void reset(Storage *storage) {
		static_wrapper<0>::instance_.reset(storage);
	}

	/*!
	 *  \brief Compares handle and string
	 *
	 *  Expected behaviour: like operator==
	 *  As a dominant operation, it should be efficient (e.g. hash comparison).
	 *  Passed handle is required to be bound to a valid string.
	 */
	virtual bool compare(const Handle &handle, const HString &str) = 0;

	/*!
	 *  \brief Extracts standard string from handle
	 */
	virtual ::std::string get(const Handle &handle) = 0;

	/*!
	 *  \brief Creates a deep copy of handle
	 *
	 *  Passed handle is required to be bound to a valid string.
	 */
	virtual void copy(Handle &handle, const Handle &other) = 0;

	/*!
	 *  \brief Binds hstring to handle
	 *
	 *  Passed handle is required to be unbound.
	 */
	virtual void bind(Handle &handle, const HString &str) = 0;

	/*!
	 *  \brief Unbinds hstring from handle
	 *
	 *  Passed handle is required to be bound to a valid string.
	 */
	virtual void unbind(Handle &handle) = 0;
	virtual ::std::string name() const = 0;

private:
	static ::std::unique_ptr<Storage> instance_;
};

template<int N>
::std::unique_ptr<Storage> Storage::static_wrapper<N>::instance_;


/*! \brief Class representing handle to String
 *
 *  This class represents a generic handle for String kept in storage.
 *  Data kept inside a handle should be interpreted by storage implementations.
 */
class Handle {
public:
	typedef uint64_t ValueType;
	typedef uint16_t HashType;

	constexpr static ValueType kHashShift = 8 * (sizeof(ValueType) - sizeof(HashType));
	constexpr static ValueType kMask = ((static_cast<ValueType>(1) << kHashShift) - static_cast<ValueType>(1));

	Handle() : data_() {
	}

	explicit Handle(ValueType data) noexcept : data_(data) {
	}

	Handle(const Handle &handle) : data_() {
		if (handle.data_) {
			Storage::instance().copy(*this, handle);
		}
	}

	Handle(Handle &&handle) noexcept : data_(handle.data_) {
		handle.data_ = 0;
	}

	explicit Handle(const HString &str) : data_() {
		Storage::instance().bind(*this, str);
	}

	explicit Handle(const ::std::string &str) : data_() {
		Storage::instance().bind(*this, HString(str));
	}

	~Handle() {
		if (data_) {
			Storage::instance().unbind(*this);
		}
	}

	Handle &operator=(const Handle &handle) {
		if (handle.data_) {
			if (data_) {
				Storage::instance().unbind(*this);
				data_ = 0;
			}
			Storage::instance().copy(*this, handle);
		} else if (data_) {
			Storage::instance().unbind(*this);
			data_ = 0;
		}
		return *this;
	}

	Handle &operator=(Handle &&handle) noexcept {
		data_ = handle.data_;
		handle.data_ = 0;
		return *this;
	}

	Handle &operator=(const HString &str) {
		set(str);
		return *this;
	}

	explicit operator ::std::string() const {
		if (data_) {
			return Storage::instance().get(*this);
		}
		return ::std::string();
	}

	explicit operator HString() const {
		return get();
	}

	bool empty() const {
		return data_ == 0;
	}

	HString get() const {
		if (data_) {
			return HString{Storage::instance().get(*this)};
		}
		return HString();
	}

	Handle &set(const HString &str) {
		if (data_) {
			Storage::instance().unbind(*this);
			data_ = 0;
		}
		Storage::instance().bind(*this, str);
		return *this;
	}

	ValueType &data() {
		return data_;
	}

	const ValueType &data() const {
		return data_;
	}

	void unlink() {
		data_ = 0;
	}

	/*!
	 * \brief Extracts hash from handle
	 *
	 * This function expects hash to be encoded on first 16 bits
	 * of data_ field.
	 */
	HashType hash() const {
		return data_ >> kHashShift;
	}

protected:
	ValueType data_;
};

/* Operators using fast hash comparison */
inline bool operator==(const Handle &handle, const HString &str) {
	if (handle.empty()) {
		return false;
	}
	return Storage::instance().compare(handle, str);
}

inline bool operator==(const HString &str, const Handle &handle) {
	if (handle.empty()) {
		return false;
	}
	return Storage::instance().compare(handle, str);
}

inline bool operator!=(const Handle &handle, const HString &str) {
	if (handle.empty()) {
		return true;
	}
	return !Storage::instance().compare(handle, str);
}

inline bool operator!=(const HString &str, const Handle &handle) {
	if (handle.empty()) {
		return true;
	}
	return !Storage::instance().compare(handle, str);
}

/* Operators using deep comparison */
inline bool operator<(const Handle &handle, const HString &str) {
	return static_cast< ::std::string>(handle) < str;
}

inline bool operator<(const HString &str, const Handle &handle) {
	return static_cast< ::std::string>(handle) > str;
}

inline bool operator<=(const Handle &handle, const HString &str) {
	return static_cast< ::std::string>(handle) <= str;
}

inline bool operator<=(const HString &str, const Handle &handle) {
	return static_cast< ::std::string>(handle) >= str;
}

inline bool operator>(const Handle &handle, const HString &str) {
	return static_cast< ::std::string>(handle) > str;
}

inline bool operator>(const HString &str, const Handle &handle) {
	return static_cast< ::std::string>(handle) < str;
}

inline bool operator>=(const Handle &handle, const HString &str) {
	return static_cast< ::std::string>(handle) >= str;
}

inline bool operator>=(const HString &str, const Handle &handle) {
	return static_cast< ::std::string>(handle) <= str;
}

} // hstorage
