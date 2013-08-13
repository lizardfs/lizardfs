

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "config.h"
#include "event.h"
#include "main.h"

#ifdef HAVE_EPOLL
#include <sys/epoll.h>
#endif
#ifdef HAVE_KQUEUE
#include <sys/event.h>
#endif

#define POLLMAX 1024

typedef struct _event {
    short mask;
    short pdescpos;
    event_cb fun;
    void *data;
} event;

static int initialized=0;
#if defined(HAVE_EPOLL) || defined(HAVE_KQUEUE)
static int pollfd=-1;
static int pollfdpdescpos=-1;
#endif
static int maxfd=-1;
static event pollevents[MFSMAXFILES+1];

int event_init() {
    if (initialized) return 0;
    initialized = 1;
    memset(pollevents, 0, sizeof(pollevents));
#ifdef HAVE_EPOLL    
    pollfd = epoll_create(MFSMAXFILES);    
    if (pollfd==-1) {
        fprintf(stderr, "create epoll failed: %s\n", strerror(errno));
        return -1;
    }
#endif    
#ifdef HAVE_KQUEUE
    pollfd = kqueue();
    if (pollfd==-1) {
        fprintf(stderr, "create kqueue failed: %s\n", strerror(errno));
        return -1;
    }
#endif
    return 0;
}

int event_add(int fd,int mask,event_cb fun,void *data) {
#ifdef HAVE_EPOLL
    struct epoll_event ee;
    ee.events = EPOLLIN|EPOLLERR|EPOLLHUP;
    if (mask & POLLOUT) ee.events |= EPOLLOUT;
    ee.data.fd = fd;
    if (epoll_ctl(pollfd, pollevents[fd].fun?EPOLL_CTL_MOD:EPOLL_CTL_ADD,fd,&ee)==-1
        && errno!=EEXIST) {
        fprintf(stderr, "epoll_ctl(%d) failed: %s\n",fd,strerror(errno));
        return -1;
    }
#endif
#ifdef HAVE_KQUEUE 
    struct kevent ev;
    uint16_t filter = EVFILT_READ;
    if (mask & POLLOUT) filter |= EVFILT_WRITE;
    EV_SET(&ev, fd, filter, EV_ADD, 0, 0, data);
    if (kevent(pollfd, &ev, 1, NULL, 0, 0)==-1){
        fprintf(stderr, "kevent(%d) failed: %s\n",fd,strerror(errno));
        return -1;
    }
#endif
    pollevents[fd].mask = mask;
    pollevents[fd].fun = fun;
    pollevents[fd].data = data;
    if (fd>maxfd) maxfd=fd;
    return 0;
}

int event_del(int fd) {
    if (!pollevents[fd].fun) return 0;
#ifdef HAVE_EPOLL
    struct epoll_event ee;
    if (epoll_ctl(pollfd,EPOLL_CTL_DEL,fd,&ee) == -1
            && errno != ENOENT && errno != EBADF) {
        fprintf(stderr, "epoll_ctl(DEL,%d) failed: %s\n",fd,strerror(errno));
        return -1;
    }
#endif    
#ifdef HAVE_KQUEUE
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ|EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    if (kevent(pollfd,&ev,1,NULL,0,0)==-1 && errno!=ENOENT) {
        fprintf(stderr, "del fd %d failed: %s\n", fd, strerror(errno));
        return -1;
    }
#endif    
    pollevents[fd].mask = 0;
    pollevents[fd].fun = NULL;
    pollevents[fd].data = NULL;
    return 0;
}

void event_desc(struct pollfd *pdesc,uint32_t *ndesc) {
#if defined (HAVE_EPOLL) || defined (HAVE_KQUEUE)
    pollfdpdescpos = *ndesc;
    pdesc[pollfdpdescpos].fd = pollfd;
    pdesc[pollfdpdescpos].events = POLLIN;
    *ndesc = pollfdpdescpos+1;
#else
    int i, pos=*ndesc;
    for (i=0; i<=maxfd; i++) {
        if (pollevents[i].fun) {
            pdesc[pos].fd = i;
            pdesc[pos].events = pollevents[i].mask;
            pollevents[i].pdescpos = pos;
            pos++;
        }
    }
    *ndesc = pos;
#endif
}

void event_serve(struct pollfd *pdesc) {
    int i;
#if defined (HAVE_EPOLL) || defined (HAVE_KQUEUE)
    int fd, numevents = 0;
    if (pollfdpdescpos<0 || !(pdesc[pollfdpdescpos].revents & (POLLIN|POLLOUT))) {
        return;
    }

#ifdef HAVE_EPOLL
    struct epoll_event evs[POLLMAX];
    numevents = epoll_wait(pollfd,evs,POLLMAX,0);
#endif
#ifdef HAVE_KQUEUE
    struct kevent evs[POLLMAX];
    numevents = kevent(pollfd,NULL,0,evs,POLLMAX,NULL);
#endif    
    for (i=0; i<numevents; i++) {
        short mask=0;
#ifdef HAVE_EPOLL    
        fd = evs[i].data.fd;
        if (evs[i].events & EPOLLIN) mask |= POLLIN;
        if (evs[i].events & (EPOLLERR|EPOLLHUP)) mask |= POLLHUP;
        if (evs[i].events & EPOLLOUT) mask |= POLLOUT;
#endif
#ifdef HAVE_KQUEUE
        fd = evs[i].ident;
        if (evs[i].filter == EVFILT_READ) mask |= POLLIN;
        if (evs[i].flags & EV_EOF) mask |= POLLHUP;
        if (evs[i].filter == EVFILT_WRITE) mask |= POLLOUT;
#endif
        pollevents[fd].fun(fd,mask,pollevents[fd].data);
    }
#else
    for (i=0; i<=maxfd; i++) {
        event *e = &pollevents[i];
        if (e->fun && pdesc[e->pdescpos].revents) {
            e->fun(i,pdesc[e->pdescpos].revents,e->data);
        }
    }
#endif
}
