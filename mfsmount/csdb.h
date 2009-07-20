#ifndef _CSDB_H_
#define _CSDB_H_

#include <inttypes.h>

void csdb_init(void);
uint32_t csdb_getreadcnt(uint32_t ip,uint16_t port);
uint32_t csdb_getwritecnt(uint32_t ip,uint16_t port);
uint32_t csdb_getopcnt(uint32_t ip,uint16_t port);
void csdb_readinc(uint32_t ip,uint16_t port);
void csdb_readdec(uint32_t ip,uint16_t port);
void csdb_writeinc(uint32_t ip,uint16_t port);
void csdb_writedec(uint32_t ip,uint16_t port);

#endif
