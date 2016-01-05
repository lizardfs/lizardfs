/*
   Copyright 2016 Skytechnology sp. z o.o.

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

#include <cstdint>
#include <iostream>

#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >=8)

#if defined(LIZARDFS_HAVE_MULTIVERSION_FUNCTIONS)

__attribute__((target("default")))
void ec_encode_data(int len, int srcs, int dests, uint8_t *v, uint8_t **src, uint8_t **dest) {
	for (int l = 0; l < dests; l++) {
		for (int i = 0; i < len; i++) {
			uint8_t s = 0;
			uint8_t *tbl = v;
			for (int j = 0; j < srcs; j++) {
				uint8_t a = src[j][i];
				uint8_t *tbl_lo = tbl;
				uint8_t *tbl_hi = tbl + 16;

				s ^= tbl_lo[a & 0xF] ^ tbl_hi[a >> 4];

				tbl += 32;
			}

			dest[l][i] = s;
		}
		v += srcs * 32;
	}
}

__attribute__((target("ssse3")))
void ec_encode_data(int len, int srcs, int dests, uint8_t *v, uint8_t **src, uint8_t **dest) {
	typedef uint8_t v16u __attribute__((vector_size(16)));
	typedef uint16_t v8ui __attribute__((vector_size(16)));
	typedef uint8_t v16u_unaligned __attribute__((vector_size(16), aligned(1)));
	typedef uint16_t v8ui_unaligned __attribute__((vector_size(16), aligned(1)));

	for (int l = 0; l < dests; l++) {
		int i = 0;

		for (; (i + 16) <= len; i += 16) {
			uint8_t *tbl = v;
			v16u s = {0};
			for (int j = 0; j < srcs; j++) {
				v8ui a = *(v8ui_unaligned *)(src[j] + i);
				v16u tbl_lo = *(v16u *)tbl;
				v16u tbl_hi = *(v16u *)(tbl + 16);

				v16u mask_lo = (v16u)(a & 0xF0F);
				v16u mask_hi = (v16u)((a >> 4) & 0xF0F);

				s ^= __builtin_shuffle(tbl_lo, mask_lo) ^ __builtin_shuffle(tbl_hi, mask_hi);

				tbl += 32;
			}

			*(v16u_unaligned *)(dest[l] + i) = s;
		}

		for (; i < len; i++) {
			uint8_t s = 0;
			uint8_t *tbl = v;
			for (int j = 0; j < srcs; j++) {
				uint8_t a = src[j][i];
				uint8_t *tbl_lo = tbl;
				uint8_t *tbl_hi = tbl + 16;

				s ^= tbl_lo[a & 0xF] ^ tbl_hi[a >> 4];

				tbl += 32;
			}

			dest[l][i] = s;
		}
		v += srcs * 32;
	}
}

__attribute__((target("avx")))
void ec_encode_data(int len, int srcs, int dests, uint8_t *v, uint8_t **src, uint8_t **dest) {
	typedef uint8_t v16u __attribute__((vector_size(16)));
	typedef uint16_t v8ui __attribute__((vector_size(16)));
	typedef uint8_t v16u_unaligned __attribute__((vector_size(16), aligned(1)));
	typedef uint16_t v8ui_unaligned __attribute__((vector_size(16), aligned(1)));

	for (int l = 0; l < dests; l++) {
		int i = 0;

		for (; (i + 16) <= len; i += 16) {
			uint8_t *tbl = v;
			v16u s = {0};
			for (int j = 0; j < srcs; j++) {
				v8ui a = *(v8ui_unaligned *)(src[j] + i);
				v16u tbl_lo = *(v16u *)tbl;
				v16u tbl_hi = *(v16u *)(tbl + 16);

				v16u mask_lo = (v16u)(a & 0xF0F);
				v16u mask_hi = (v16u)((a >> 4) & 0xF0F);

				s ^= __builtin_shuffle(tbl_lo, mask_lo) ^ __builtin_shuffle(tbl_hi, mask_hi);

				tbl += 32;
			}

			*(v16u_unaligned *)(dest[l] + i) = s;
		}

		for (; i < len; i++) {
			uint8_t s = 0;
			uint8_t *tbl = v;
			for (int j = 0; j < srcs; j++) {
				uint8_t a = src[j][i];
				uint8_t *tbl_lo = tbl;
				uint8_t *tbl_hi = tbl + 16;

				s ^= tbl_lo[a & 0xF] ^ tbl_hi[a >> 4];

				tbl += 32;
			}

			dest[l][i] = s;
		}

		v += srcs * 32;
	}
}

#if __GNUC__ >= 5

#include "immintrin.h"

__attribute__((target("avx2")))
void ec_encode_data(int len, int srcs, int dests, uint8_t *v, uint8_t **src, uint8_t **dest) {
	typedef uint8_t v32u __attribute__((vector_size(32)));
	typedef uint16_t v16ui __attribute__((vector_size(32)));
	typedef uint8_t v32u_unaligned __attribute__((vector_size(32), aligned(1)));
	typedef uint16_t v16ui_unaligned __attribute__((vector_size(32), aligned(1)));

	for (int l = 0; l < dests; l++) {
		int i = 0;

		for (; (i + 32) <= len; i += 32) {
			uint8_t *tbl = v;
			v32u s = {0};
			for (int j = 0; j < srcs; j++) {
				v16ui a = *(v16ui_unaligned *)(src[j] + i);

				v32u tbl_lo = *(v32u *)tbl;

				v32u tbl_hi =
				    (v32u)_mm256_permute2x128_si256((__m256i)tbl_lo, (__m256i)tbl_lo, 0x11);
				tbl_lo = (v32u)_mm256_permute2x128_si256((__m256i)tbl_lo, (__m256i)tbl_lo, 0x00);

				v32u mask_lo = (v32u)(a & 0xF0F);
				v32u mask_hi = (v32u)((a >> 4) & 0xF0F);

				s ^= (v32u)_mm256_shuffle_epi8((__m256i)tbl_lo, (__m256i)mask_lo) ^
				     (v32u)_mm256_shuffle_epi8((__m256i)tbl_hi, (__m256i)mask_hi);

				tbl += 32;
			}

			*(v32u_unaligned *)(dest[l] + i) = s;
		}

		for (; i < len; i++) {
			uint8_t s = 0;
			uint8_t *tbl = v;
			for (int j = 0; j < srcs; j++) {
				uint8_t a = src[j][i];
				uint8_t *tbl_lo = tbl;
				uint8_t *tbl_hi = tbl + 16;

				s ^= tbl_lo[a & 0xF] ^ tbl_hi[a >> 4];

				tbl += 32;
			}

			dest[l][i] = s;
		}
		v += srcs * 32;
	}
}

#endif

#else

void ec_encode_data(int len, int srcs, int dests, uint8_t *v, uint8_t **src, uint8_t **dest) {
	typedef uint8_t v16u __attribute__((vector_size(16)));
	typedef uint16_t v8ui __attribute__((vector_size(16)));
	typedef uint8_t v16u_unaligned __attribute__((vector_size(16), aligned(1)));
	typedef uint16_t v8ui_unaligned __attribute__((vector_size(16), aligned(1)));

	for (int l = 0; l < dests; l++) {
		int i = 0;

		for (; (i + 16) <= len; i += 16) {
			uint8_t *tbl = v;
			v16u s = {0};
			for (int j = 0; j < srcs; j++) {
				v8ui a = *(v8ui_unaligned *)(src[j] + i);
				v16u tbl_lo = *(v16u *)tbl;
				v16u tbl_hi = *(v16u *)(tbl + 16);

				v16u mask_lo = (v16u)(a & 0xF0F);
				v16u mask_hi = (v16u)((a >> 4) & 0xF0F);

				s ^= __builtin_shuffle(tbl_lo, mask_lo) ^ __builtin_shuffle(tbl_hi, mask_hi);

				tbl += 32;
			}

			*(v16u_unaligned *)(dest[l] + i) = s;
		}

		for (; i < len; i++) {
			uint8_t s = 0;
			uint8_t *tbl = v;
			for (int j = 0; j < srcs; j++) {
				uint8_t a = src[j][i];
				uint8_t *tbl_lo = tbl;
				uint8_t *tbl_hi = tbl + 16;

				s ^= tbl_lo[a & 0xF] ^ tbl_hi[a >> 4];

				tbl += 32;
			}

			dest[l][i] = s;
		}

		v += srcs * 32;
	}
}

#endif

#else

void ec_encode_data(int len, int srcs, int dests, uint8_t *v, uint8_t **src, uint8_t **dest) {
	for (int l = 0; l < dests; l++) {
		for (int i = 0; i < len; i++) {
			uint8_t s = 0;
			uint8_t *tbl = v;
			for (int j = 0; j < srcs; j++) {
				uint8_t a = src[j][i];
				uint8_t *tbl_lo = tbl;
				uint8_t *tbl_hi = tbl + 16;

				s ^= tbl_lo[a & 0xF] ^ tbl_hi[a >> 4];

				tbl += 32;
			}

			dest[l][i] = s;
		}
		v += srcs * 32;
	}
}

#endif
