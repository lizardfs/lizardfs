#ifndef _DIRATTRCACHE_H_
#define _DIRATTRCACHE_H_

void* dcache_new(const struct fuse_ctx *ctx,uint32_t parent,const uint8_t *dbuff,uint32_t dsize);
void dcache_release(void *r);
uint8_t dcache_lookup(const struct fuse_ctx *ctx,uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]);
uint8_t dcache_getattr(const struct fuse_ctx *ctx,uint32_t inode,uint8_t attr[35]);

#endif
