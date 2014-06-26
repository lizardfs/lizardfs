#include "common/platform.h"
#include "personality.h"

namespace master {

static Personality gPersonality = Personality::kMaster;

Personality getPersonality() {
	return gPersonality;
}

void setPersonality(Personality personality) {
	gPersonality = personality;
}

}

