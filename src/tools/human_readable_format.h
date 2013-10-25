#ifndef LIZARDFS_TOOLS_HUMAN_READABLE_FORMAT_H_
#define LIZARDFS_TOOLS_HUMAN_READABLE_FORMAT_H_

#include <cstdint>
#include <string>

std::string convertToSi(uint64_t number);
std::string convertToIec(uint64_t number);

#endif // LIZARDFS_TOOLS_HUMAN_READABLE_FORMAT_H_

