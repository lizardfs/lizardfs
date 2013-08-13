

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

static int pollfd=-1;
static int pollfdpdescpos=-1;
static event pollevents[MFSMAXFILES+1];

int event_init() {
    if (pollfd>=0) return 0;
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

int event_add(int fd,int flag,event_cb fun,void *data) {
#ifdef HAVE_EPOLL
    struct epoll_event ee;
    ee.events = EPOLLIN|EPOLLERR|EPOLLHUP;
    if (flag & AE_WRITE) ee.events |= EPOLLOUT;
    ee.data.fd = fd;
    if (epoll_ctl(pollfd, pollevents[fd].fd?EPOLL_CTL_MOD:EPOLL_CTL_ADD,fd,&ee)==-1
        && errno!=EEXIST) {
        fprintf(stderr, "epoll_ctl(%d) failed: %s\n",fd,strerror(errno));
        return -1;
    }
#endif
#ifdef HAVE_KQUEUE 
    struct kevent ev;
    uint16_t filter = EVFILT_READ;
    if (flag & AE_WRITE) filter |= EVFILT_WRITE;
    EV_SET(&ev, fd, filter, EV_ADD, 0, 0, data);
    if (kevent(pollfd, &ev, 1, NULL, 0, 0)==-1){
        fprintf(stderr, "kevent(%d) failed: %s\n",fd,strerror(errno));
        return -1;
    }
#endif
    pollevents[fd].fd = fd;
    pollevents[fd].mask = flag;
    pollevents[fd].fun = fun;
    pollevents[fd].data = data;
    return 0;
}

int event_del(int fd) {
    if (!pollevents[fd].fd) return 0;
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
    pollevents[fd].fd = 0;
    pollevents[fd].mask = 0;
    pollevents[fd].fun = NULL;
    pollevents[fd].data = NULL;
    return 0;
}

int event_poll(event *events) {
    int j, fd, numevents = 0;
#ifdef HAVE_EPOLL
    struct epoll_event evs[POLLMAX];
    numevents = epoll_wait(pollfd,evs,POLLMAX,0);
#endif
#ifdef HAVE_KQUEUE
    struct kevent evs[POLLMAX];
    numevents = kevent(pollfd,NULL,0,evs,POLLMAX,NULL);
#endif    
    for (j=0; j<numevents; j++) {
        int mask=0;
#ifdef HAVE_EPOLL    
        fd = evs[j].data.fd;
        if (evs[j].events & EPOLLIN) mask |= AE_READ;
        if (evs[j].events & EPOLLOUT) mask |= AE_WRITE;
        if (evs[j].events & (EPOLLERR|EPOLLHUP)) mask |= AE_READ|AE_EOF;
#endif
#ifdef HAVE_KQUEUE
        fd = evs[j].ident;
        if (evs[j].filter == EVFILT_READ) {
            mask |= AE_READ;
            if (evs[j].flags & EV_EOF) mask |= AE_EOF;
        }
        if (evs[j].filter == EVFILT_WRITE) mask |= AE_WRITE;
#endif
        events[j].fd = fd;
        events[j].mask = mask;
        events[j].fun = pollevents[fd].fun;
        events[j].data = pollevents[fd].data;
    }
    return numevents;
}

void event_desc(struct pollfd *pdesc,uint32_t *ndesc) {
    if (pollfd<0) return;
#if defined (HAVE_EPOLL) || defined (HAVE_KQUEUE)
    pollfdpdescpos = *ndesc;
    pdesc[pollfdpdescpos].fd = pollfd;
    pdesc[pollfdpdescpos].events = POLLIN;
    *ndesc = pollfdpdescpos+1;
#endif
}

void event_serve(struct pollfd *pdesc) {
    if (pollfd<0) return;
#if defined (HAVE_EPOLL) || defined (HAVE_KQUEUE)
    int i, ret;
    event evs[POLLMAX];
    if (pollfdpdescpos<0 || !(pdesc[pollfdpdescpos].revents & (POLLIN|POLLOUT))) {
        return;
    }
    ret = event_poll(evs);
    for (i=0; i<ret; i++) {
        evs[i].fun(evs[i].fd,evs[i].mask,evs[i].data);
    }
#endif
}
