#ifndef _MD5_H_
#define _MD5_H_

#include <inttypes.h>

typedef struct _md5ctx {
	uint32_t state[4];
	uint32_t count[2];
	uint8_t buffer[64];
} md5ctx;

void md5_init(md5ctx *ctx);
void md5_update(md5ctx *ctx,const uint8_t *buff,uint32_t leng);
void md5_final(uint8_t digest[16],md5ctx *ctx);

#endif
