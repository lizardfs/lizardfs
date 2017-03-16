/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "personality.h"

#include <algorithm>

#include "common/cfg.h"
#include "common/exceptions.h"
#include "common/event_loop.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/slogger.h"

namespace metadataserver {

static const std::string kMasterCmdOption = "initial-personality=master";
static const std::string kShadowCmdOption = "initial-personality=shadow";
static const std::string kClusterManagedCmdOption = "ha-cluster-managed";
static const std::string kClusterManagedPersonality = "ha-cluster-managed";

static Personality gPersonality = Personality::kMaster;
static std::vector<void(*)(void)> gChangePersonalityReloadFunctions;

Personality getPersonality() {
	return gPersonality;
}

void setPersonality(Personality personality) {
	gPersonality = personality;
}

static bool personalityInConfigIsHaClusterManaged() {
	std::string p = cfg_get("PERSONALITY", "not " + kClusterManagedPersonality);
	std::transform(p.begin(), p.end(), p.begin(), tolower);
	return p == kClusterManagedPersonality;
}

/*! \brief Load personality settings from the config file.
 *
 * \return Current (newly loaded) metadata server personality.
 */
static Personality loadNonHaClusterPersonality() {
	const std::string kMaster = "master";
	const std::string kShadow = "shadow";
	std::string p = cfg_get("PERSONALITY", kMaster);
	std::transform(p.begin(), p.end(), p.begin(), tolower);
	if (p == kMaster) {
		return Personality::kMaster;
	} else if (p == kShadow) {
		return Personality::kShadow;
	} else if (personalityInConfigIsHaClusterManaged()) {
		return gPersonality;
	} else {
		throw ConfigurationException("bad personality");
	}
}

void registerFunctionCalledOnPromotion(void(*f)(void)) {
	gChangePersonalityReloadFunctions.push_back(f);
}

void promoteToMaster() {
	lzfs_pretty_syslog(LOG_INFO, "changing metadataserver personality from Shadow to Master");
	for (auto& f : gChangePersonalityReloadFunctions) {
		f();
	}
	setPersonality(Personality::kMaster);
}

void personality_reload(void) {
	// First verify if we are not trying to switch from ha-managed to non-ha-managed personality
	bool configHaManaged = personalityInConfigIsHaClusterManaged();
	bool optionHaManaged = main_has_extra_argument(kClusterManagedCmdOption,
			CaseSensitivity::kIgnore);
	if (configHaManaged != optionHaManaged) {
		lzfs_pretty_syslog(LOG_ERR,
				"metadata server personality cannot be switched between %s and master/shadow",
				kClusterManagedPersonality.c_str());
		return;
	}

	// For ha-cluster managed personality we don't need to do anything
	if (optionHaManaged) {
		return;
	}

	// For non ha-cluster managed personality -- reload it
	try {
		Personality personality = loadNonHaClusterPersonality();
		if (personality != gPersonality) {
			if (personality == Personality::kMaster) {
				promoteToMaster();
			} else {
				lzfs_pretty_syslog(LOG_ERR,
						"trying to preform forbidden personality change from Master to Shadow");
			}
		}
	} catch (const ConfigurationException& e) {
		/* reload shall not break instance */
		lzfs_pretty_syslog(LOG_ERR, "bad configuration: `%s'", e.what());
	}
}

bool promoteAutoToMaster() {
	if (!personalityInConfigIsHaClusterManaged()) {
		return false;
	}
	if (gPersonality != Personality::kShadow) {
		return false;
	}
	promoteToMaster();
	return true;
}

int personality_validate() {
	static std::string haAdvise = "This installation is managed by HA cluster,"
			" one should manipulate metadata servers only using lizardfs-cluster-manager.";
	static std::string nonHaAdvise = "Metadata server configuration states that this installation"
			" is NOT managed by HA cluster. In case if it is supposed to be managed by a cluster"
			" change the configuration (change the personality defined in " + cfg_filename() +
			" to " + kClusterManagedPersonality + "), otherwise stop using "
			+ kClusterManagedCmdOption + " command line option.";
	bool configHaManaged = personalityInConfigIsHaClusterManaged();
	bool optionHaManaged = main_has_extra_argument(kClusterManagedCmdOption,
			CaseSensitivity::kIgnore);
	if (!configHaManaged && optionHaManaged) {
		throw ConfigurationException(nonHaAdvise);
	}
	if (configHaManaged && !optionHaManaged) {
		throw ConfigurationException(haAdvise);
	}
	return 0;
}

int personality_init() {
#ifndef METARESTORE
	eventloop_reloadregister(personality_reload);
#endif /* #ifndef METARESTORE */
	bool master = main_has_extra_argument(kMasterCmdOption, CaseSensitivity::kIgnore);
	bool shadow = main_has_extra_argument(kShadowCmdOption, CaseSensitivity::kIgnore);
	if (shadow && master) {
		throw ConfigurationException("Command line options " +
				kMasterCmdOption + " and " + kShadowCmdOption + " are mutually exclusive");
	}
	if (personalityInConfigIsHaClusterManaged()) {
		if (master) {
			setPersonality(Personality::kMaster);
		} else if (shadow){
			setPersonality(Personality::kShadow);
		} else {
			throw ConfigurationException("Missing "
					+ kMasterCmdOption + " or " + kShadowCmdOption + " command line option");
		}
	} else {
		if (shadow || master) {
			throw ConfigurationException("Command line options " +
					kMasterCmdOption + " and " + kShadowCmdOption + " can't be used for non " +
					"ha-cluster managed installations");
		} else {
			setPersonality(loadNonHaClusterPersonality());
		}
	}
	return 0;
}

bool isMaster() {
	return gPersonality == Personality::kMaster;
}

}

