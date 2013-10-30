#ifndef _MERGER_H_
#define _MERGER_H_

#include <inttypes.h>

int merger_start(uint32_t files,char **filenames,uint64_t maxhole);
int merger_loop(void);

#endif
