#pragma once

#include "config.h"

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
