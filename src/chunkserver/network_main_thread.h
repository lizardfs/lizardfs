#pragma once

#include "common/platform.h"

#include <inttypes.h>

int mainNetworkThreadInit(void);

uint32_t mainNetworkThreadGetListenIp();
uint16_t mainNetworkThreadGetListenPort();
