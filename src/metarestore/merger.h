#pragma once

#include "config.h"

#include <cstdint>
#include <string>
#include <vector>

int merger_start(const std::vector<std::string>& filenames, uint64_t maxhole);
int merger_loop(void);
