#pragma once

#include "common/platform.h"

#include <inttypes.h>

int mainNetworkThreadInit(void);
int mainNetworkThreadInitThreads(void);

uint32_t mainNetworkThreadGetListenIp();
uint16_t mainNetworkThreadGetListenPort();
