#ifndef LIZARDFS_MFSCOMMON_LIST_H_
#define LIZARDFS_MFSCOMMON_LIST_H_

#include <stdlib.h>
#include <cstdint>

template<class MooseFSStyleList>
uint32_t list_length(MooseFSStyleList* list) {
	uint32_t result = 0;
	for (auto element = list; element != NULL; element = element->next) {
		++result;
	}
	return result;
}

#endif /* LIZARDFS_MFSCOMMON_LIST_H_ */
