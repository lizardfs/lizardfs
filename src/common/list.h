#pragma once
#include "common/platform.h"
#include <stdlib.h>
#include <cstdint>

template<class LizardFSStyleList>
uint32_t list_length(LizardFSStyleList* list) {
	uint32_t result = 0;
	for (auto element = list; element != NULL; element = element->next) {
		++result;
	}
	return result;
}
