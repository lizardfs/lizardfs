
#ifndef _EVENT_H_
#define _EVENT_H_

#include <inttypes.h>
#include <poll.h>

typedef void (*event_cb)(int fd,int mask,void *ptr);

int event_init();
int event_add(int fd,int mask,event_cb fun,void *data);
int event_del(int fd);
void event_desc(struct pollfd *pdesc,uint32_t *ndesc);
void event_serve(struct pollfd *pdesc);

#endif // _EVENT_H_
