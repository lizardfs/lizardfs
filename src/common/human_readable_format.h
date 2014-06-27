#pragma once

#include "common/platform.h"

#include <cstdint>
#include <string>

std::string convertToSi(uint64_t number);
std::string convertToIec(uint64_t number);
std::string ipToString(uint32_t ip);
std::string timeToString(time_t time);
std::string bpsToString(uint64_t bytes, uint64_t usec);
