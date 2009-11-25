#ifndef _ACL_H_
#define _ACL_H_

#include <stdio.h>
#include <inttypes.h>

uint32_t acl_info_size(void);
void acl_info_data(uint8_t *buff);
uint8_t acl_check(uint32_t ip,uint32_t version,uint8_t meta,const uint8_t *path,const uint8_t rndcode[32],const uint8_t passcode[16],uint8_t *sesflags,uint32_t *rootuid,uint32_t *rootgid,uint32_t *mapalluid,uint32_t *mapallgid);
int acl_init(FILE *msgfd);

#endif
