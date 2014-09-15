#pragma once

#include "common/platform.h"

#include <cstdint>
#include <string>
#include <vector>

int merger_start(const std::vector<std::string>& filenames, uint64_t maxhole);
uint8_t merger_loop(void);
