#pragma once

#include "common/platform.h"

namespace master {

enum class Personality {
	kMaster,
	kShadow
};

Personality getPersonality();
void setPersonality(Personality);

}

