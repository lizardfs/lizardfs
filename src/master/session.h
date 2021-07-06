#pragma once

#include <inttypes.h>

#include "common/generic_lru_cache.h"
#include "common/platform.h"
#include "master/filesystem.h"

#define SESSION_STATS 16

// opened files
typedef struct filelist {
    uint32_t inode;
    struct filelist *next;
} filelist;

struct session {
    typedef GenericLruCache<uint32_t, FsContext::GroupsContainer, 1024> GroupCache;

    uint32_t sessionid;
    char *info;
    uint32_t peerip;
    uint8_t newsession;
    uint8_t sesflags;
    uint8_t mingoal;
    uint8_t maxgoal;
    uint32_t mintrashtime;
    uint32_t maxtrashtime;
    uint32_t rootuid;
    uint32_t rootgid;
    uint32_t mapalluid;
    uint32_t mapallgid;
    uint32_t rootinode;
    uint32_t disconnected;  // 0 = connected ; other = disconnection timestamp
    uint32_t nsocks;        // >0 - connected (number of active connections) ; 0 - not connected
    std::array<uint32_t, SESSION_STATS> currentopstats;
    std::array<uint32_t, SESSION_STATS> lasthouropstats;
    GroupCache group_cache;
    filelist *openedfiles;
    struct session *next;

    session()
            : sessionid(),
              info(),
              peerip(),
              newsession(),
              sesflags(),
              mingoal(GoalId::kMin),
              maxgoal(GoalId::kMax),
              mintrashtime(),
              maxtrashtime(std::numeric_limits<uint32_t>::max()),
              rootuid(),
              rootgid(),
              mapalluid(),
              mapallgid(),
              rootinode(SPECIAL_INODE_ROOT),
              disconnected(),
              nsocks(),
              currentopstats(),
              lasthouropstats(),
              group_cache(),
              openedfiles(),
              next() {
    }
};
