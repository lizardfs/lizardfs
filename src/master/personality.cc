#include "common/platform.h"
#include "personality.h"

#include <algorithm>

#include "common/cfg.h"
#include "common/exceptions.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/slogger.h"

namespace metadataserver {

static Personality gPersonality = Personality::kMaster;

Personality getPersonality() {
	return gPersonality;
}

void setPersonality(Personality personality) {
	gPersonality = personality;
}

Personality loadPersonality() {
	const std::string kMaster = "master";
	const std::string kShadow = "shadow";
	std::string p = cfg_get("PERSONALITY", kMaster);
	std::transform(p.begin(), p.end(), p.begin(), tolower);
	Personality personality = Personality::kMaster;
	if (p == kMaster) {
		personality = Personality::kMaster;
	} else if (p == kShadow) {
		personality = Personality::kShadow;
	} else {
		throw ConfigurationException("bad personality");
	}
	return personality;
}

bool isDuringPersonalityChange() {
	metadataserver::Personality personality = metadataserver::getPersonality();
	metadataserver::Personality newPersonality = metadataserver::loadPersonality();
	sassert((newPersonality == personality) || (newPersonality == Personality::kMaster));
	return (newPersonality != personality) && (personality == Personality::kShadow);
}

void personality_reload(void) {
	try {
		Personality personality = loadPersonality();
		if (personality != gPersonality) {
			if (personality == Personality::kMaster) {
				lfs_syslog(LOG_INFO, "changing metadataserver personality from Shadow to Master");
				setPersonality(personality);
			} else {
				lfs_syslog(LOG_ERR, "trying to preform forbidden personality change from Master to Shadow");
			}
		}
	} catch (const ConfigurationException& e) {
		/* reload shall not break instance */
		lfs_arg_syslog(LOG_ERR, "bad configuration: `%s'", e.what());
	}
}

int personality_init() {
	int ret = 0;
	try {
		setPersonality(loadPersonality());
	} catch (const ConfigurationException& e) {
		ret = -1;
	}
#ifndef METARESTORE
	main_reloadregister(personality_reload);
#endif /* #ifndef METARESTORE */
	return ret;
}

bool isMaster() {
	return gPersonality == Personality::kMaster;
}

}

