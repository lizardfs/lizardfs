#ifndef _STATS_H_
#define _STATS_H_

#include <inttypes.h>

void* stats_get_subnode(void *node,const char *name);
uint64_t* stats_get_counterptr(void *node);
void stats_reset_all(void);
void stats_show_all(char **buff,uint32_t *leng);

#endif
