#ifndef _TH_SEM_H_
#define _TH_SEM_H_

#include <inttypes.h>

void* sem_new(uint32_t resources);
void sem_delete(void *sem);
uint32_t sem_getresamount(void *sem);
void sem_acquire(void *sem,uint32_t res);
int sem_tryacquire(void *sem,uint32_t res);
void sem_release(void *sem,uint32_t res);
void sem_broadcast_release(void *sem,uint32_t res);

#endif
