
#ifndef _EVENT_H_
#define _EVENT_H_

#include <inttypes.h>
#include <poll.h>

#define AE_READ 1
#define AE_WRITE 2
#define AE_EOF 4

typedef void (*event_cb)(int fd,int mask,void *ptr);

typedef struct _event {
    int fd;
    int mask;
    event_cb fun;
    void *data;
} event;

int event_init();
int event_add(int fd,int flag,event_cb fun,void *data);
int event_del(int fd);
int event_poll(event *evs);
void event_desc(struct pollfd *pdesc,uint32_t *ndesc);
void event_serve(struct pollfd *pdesc);

#endif // _EVENT_H_
