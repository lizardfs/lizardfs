#include "common/platform.h"
#include "common/block_xor.h"

#include <algorithm>
#include <cstdlib>

#include "common/massert.h"

#if defined(__GNUC__) && defined(__GNUC_MINOR__)
#  if (__GNUC__ > 4) || (__GNUC__ == 4 && __GNUC_MINOR__ >= 7)
#    define HAVE_BUILTIN_ASSUME_ALIGNED
#  endif
#endif

// We check whether dest and source are well-aligned and hope that compiler will perform XORs using
// vector instructions. Some architectures don't support unaligned vector loads and stores, others
// (e.g. x86) support them but aligned versions still are faster.
#define ALIGNMENT size_t(16)


// Assumes that dest and source are properly aligned:
static inline void blockXorAligned(uint8_t* dest, const uint8_t* source, size_t size);

// Works for any alignment of dest and source:
static inline void blockXorUnaligned(uint8_t* dest, const uint8_t* source, size_t size);


// Public interface, tries to do it as well as possible.
void blockXor(uint8_t* dest, const uint8_t* source, size_t size) {
	intptr_t d = reinterpret_cast<intptr_t>(dest);
	intptr_t s = reinterpret_cast<intptr_t>(source);
	if (d % ALIGNMENT == s % ALIGNMENT) {
		size_t unalignedPrefixSize = (ALIGNMENT - d % ALIGNMENT) % ALIGNMENT;
		if (size < unalignedPrefixSize) {
			blockXorUnaligned(dest, source, size);
		} else {
			blockXorUnaligned(dest, source, unalignedPrefixSize);
			blockXorAligned(dest + unalignedPrefixSize, source + unalignedPrefixSize,
					size - unalignedPrefixSize);
		}
	} else {
		blockXorUnaligned(dest, source, size);
	}
}

static inline void blockXorUnaligned(uint8_t* dest, const uint8_t* source, size_t size) {
	// This code compiled with gcc uses unaligned vector loads/stores (movdqu) and pxor on x86.
	for (size_t i = 0; i < size; ++i) {
		dest[i] ^= source[i];
	}
}

static inline void blockXorAligned(uint8_t* dest, const uint8_t* source, size_t size) {
	intptr_t d = reinterpret_cast<intptr_t>(dest);
	intptr_t s = reinterpret_cast<intptr_t>(source);
	sassert(d % ALIGNMENT == 0 && s % ALIGNMENT == 0);
#ifdef HAVE_BUILTIN_ASSUME_ALIGNED
	uint8_t* alignedDest =
		static_cast<uint8_t*>(__builtin_assume_aligned(dest, ALIGNMENT));
	const uint8_t* alignedSource =
		static_cast<const uint8_t*>(__builtin_assume_aligned(source, ALIGNMENT));
	// This code compiled with gcc use aligned vector loads/stores (movdqa) and pxor on x86.
	for (size_t i = 0; i < size; ++i) {
		alignedDest[i] ^= alignedSource[i];
	}
#else
	// :(
	blockXorUnaligned(dest, source, size);
#endif
}
