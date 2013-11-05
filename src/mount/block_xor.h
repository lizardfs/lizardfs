#ifndef LIZARDFS_MFSMOUNT_BLOCK_XOR_H_
#define LIZARDFS_MFSMOUNT_BLOCK_XOR_H_

#include <cstddef>
#include <cstdint>

/*
 * XOR dest in-place with source.
 *
 * Implementation will try to use vector instructions if dest and source
 * are well-aligned or, at least, the difference between them is divisible
 * by vector width.
 */
void blockXor(uint8_t* dest, const uint8_t* source, size_t size);

#endif // LIZARDFS_MFSMOUNT_BLOCK_XOR_H_
