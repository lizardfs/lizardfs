#pragma once

#include <inttypes.h>
#include <zlib.h>
#include "datapack.h"

#define PNG_SIGNATURE_SIZE 8
#define PNG_CHUNK_SIGNATURE_SIZE 4
#define PNG_CHUNK_SIZE_SIZE 4
#define PNG_CHUNK_CRC_SIZE 4
#define PNG_CHUNK_MIN_SIZE (PNG_CHUNK_SIZE_SIZE + PNG_CHUNK_SIGNATURE_SIZE + PNG_CHUNK_CRC_SIZE)

static uint32_t png_chunk_get_data_size(const uint8_t *chunk);

static inline const uint8_t *
png_first_chunk(const uint8_t *png) {
    return png + PNG_SIGNATURE_SIZE;
}

static inline uint8_t *
png_first_chunk(uint8_t *png) {
    return png + PNG_SIGNATURE_SIZE;
}

static inline const uint8_t *
png_next_chunk(const uint8_t *chunk) {
    return chunk + PNG_CHUNK_MIN_SIZE + png_chunk_get_data_size(chunk);
}

static inline uint8_t *
png_next_chunk(uint8_t *chunk) {
    return chunk + PNG_CHUNK_MIN_SIZE + png_chunk_get_data_size(chunk);
}

static inline uint32_t
png_chunk_compute_crc( const uint8_t *chunk) {
    return crc32(0,chunk + PNG_CHUNK_SIZE_SIZE, PNG_CHUNK_SIGNATURE_SIZE + png_chunk_get_data_size(chunk));
}

static inline int
png_chunk_get_crc_offset(const uint8_t *chunk) {
    return PNG_CHUNK_SIZE_SIZE + PNG_CHUNK_SIGNATURE_SIZE + png_chunk_get_data_size(chunk);
}

static inline uint32_t
png_chunk_get_crc(const uint8_t *chunk) {
    chunk += png_chunk_get_crc_offset(chunk);
    return get32bit(&chunk);
}

static inline uint32_t
png_chunk_get_data_size(const uint8_t *chunk) {
    return get32bit(&chunk);
}

static inline void
png_chunk_set_crc(uint8_t *chunk, uint32_t crc) {
    chunk += png_chunk_get_crc_offset(chunk);
    return put32bit(&chunk,crc);
}

static inline void
png_chunk_update_crc(uint8_t *chunk) {
    png_chunk_set_crc(chunk,png_chunk_compute_crc(chunk));
}

static inline int
png_chunk_verify_crc(uint8_t *chunk) {
    return png_chunk_get_crc(chunk) - png_chunk_compute_crc(chunk);
}

static inline int
png_chunk_verify_limits(const uint8_t *chunk, const uint8_t *png_end) {
    int d = (chunk + PNG_CHUNK_MIN_SIZE) - png_end;
    if (d > 0)
        return d;
    d = (chunk + PNG_CHUNK_MIN_SIZE + png_chunk_get_data_size(chunk)) - png_end;
    return d > 0 ? d : 0;
}
