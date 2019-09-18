#include "common/platform.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <algorithm>

#include "master/hstring_memstorage.h"

using namespace hstorage;

#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
	std::set<char *> *MemStorage::debug_ptr_ = nullptr;
#endif

bool MemStorage::compare(const Handle &handle, const HString &str) {
	if (hash(handle) == static_cast<HashType>(str.hash())) {
		return str == c_str(handle);
	}
	return false;
}

::std::string MemStorage::get(const Handle &handle) {
	return c_str(handle);
}

void MemStorage::copy(Handle &handle, const Handle &other) {
	char *copied = strdup(c_str(other));
	if (!copied) {
		throw std::bad_alloc();
	}
	handle.data() = encode(copied, hash(other));

#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
	if (!debug_ptr_) {
		debug_ptr_ = new std::set<char *>();
	}
	debug_ptr_->insert(copied);
#endif
}

/*
 * Binding hstring to handle:
 * 1. Copy C-style string
 * 2. Encode C-style string combined with its hash in an obfuscated pointer
 */
void MemStorage::bind(Handle &handle, const HString &str) {
	HString::size_type size = str.size() + 1;
	char *copied = (char *)malloc(size * sizeof(HString::value_type));
	if (!copied) {
		throw std::bad_alloc();
	}
	memcpy(copied, str.c_str(), size);
	handle.data() = encode(copied, str.hash());

#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
	if (!debug_ptr_) {
		debug_ptr_ = new std::set<char *>();
	}
	debug_ptr_->insert(copied);
#endif
}

/**
 * \note Works only on systems with 48-bit user space address (virtual memory).
 */
MemStorage::ValueType MemStorage::encode(const char *ptr, HashType hash) {
	return static_cast<ValueType>(reinterpret_cast<uintptr_t>(ptr))
			| (static_cast<ValueType>(hash) << kShift);
}

void MemStorage::unbind(Handle &handle) {
#if !defined(NDEBUG) || defined(LIZARDFS_TEST_POINTER_OBFUSCATION)
	char *ptr = c_str(handle);
	if (ptr) {
		if (!debug_ptr_) {
			debug_ptr_ = new std::set<char *>();
		}
		auto it = debug_ptr_->find(ptr);
		assert(it != debug_ptr_->end());
		debug_ptr_->erase(it);
	}
#endif
	free(c_str(handle));
}

::std::string MemStorage::name() const {
	return kName;
}
