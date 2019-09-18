#pragma once

#include "common/platform.h"

#include "master/hstring_storage.h"

#include <set>

namespace hstorage {

/*! \brief In-memory storage for hstring
 *
 * This class implements in-memory storage for hstring.
 * Data stored in handle is interpreted as a C-style string + 16 bits of its hash.
 */
class MemStorage : public Storage {
	static_assert(sizeof(void *) <= 8, "This class supports only <= 64bit architectures");
public:
	/* On 32bit it is possible to use 32 bits, but we limit ourselves to 16 bits
	 * to be consistent with other storage implementations.
	 */
	typedef Handle::HashType HashType;
	typedef Handle::ValueType ValueType;

	bool compare(const Handle &handle, const HString &str) override;
	::std::string get(const Handle &handle) override;
	void copy(Handle &handle, const Handle &other) override;
	void bind(Handle &handle, const HString &str) override;
	void unbind(Handle &handle) override;
	::std::string name() const override;

	static HashType hash(const Handle &handle) {
		return static_cast<HashType>(handle.data() >> kShift);
	}

	static const char *c_str(const Handle &handle) {
		return reinterpret_cast<const char *>(handle.data() & kMask);
	}

	static char *c_str(Handle &handle) {
		return reinterpret_cast<char *>(handle.data() & kMask);
	}

private:
	ValueType encode(const char *ptr, HashType hash);

	static constexpr const char *kName = "MemStorage";
	static constexpr ValueType kShift = 64 - 8 * sizeof(HashType);
	static constexpr ValueType kMask = ((static_cast<ValueType>(1) << kShift) - static_cast<ValueType>(1));

#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
	static std::set<char *> *debug_ptr_; /*!< Set with unobfuscated pointers to stored strings.
	                                            Just to make valgrind happy. */
#endif
};

} //namespace hstring
