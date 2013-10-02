#include "lizardlink.h"

LizardLinkTool::LizardLinkTool() : LizardCommand("TOOLS") {
}

void LizardLinkTool::parseOptions(const int& pArgc, char** pArgv) {
    this->m_Parameters.first = pArgc;
    this->m_Parameters.second = pArgv;
}

int LizardLinkTool::executeCommand() {

    if (m_Parameters.first == 2 && strcmp(m_Parameters.second[1], "create") == 0) {
        fprintf(stderr, "create symlinks\n");
        for(map<string, LizardTool*>::iterator iter = LIZARD_TOOLS_MAP.begin(); iter != LIZARD_TOOLS_MAP.end(); ++iter) {
            if (symlink((string(MFS_PREF) + m_ToolName).c_str(), iter->first.c_str()) < 0) {
                    perror((string("error creating symlink '") + iter->first.c_str() + "'").c_str());
            }
        }
        return 0;
    } else {
        this->usage();
    }
    return 1;
}

void LizardLinkTool::usage() {

    fprintf(stderr,"%s multi tool\n\n usage:\n\t%stools create - create symlinks	 ([%s|%s]<toolname> -> %s)\n", MFS_PREF, MFS_PREF, LIZ_PREF, MFS_PREF, m_ToolName.c_str() );
    fprintf(stderr,"\ntools:\n");

    map<string, LizardTool*>::iterator toolsIter;
    for(toolsIter = LIZARD_TOOLS_MAP.begin(); toolsIter != LIZARD_TOOLS_MAP.end(); ++toolsIter) {
           fprintf(stderr,"\t%s\n", toolsIter->first.c_str());
    }

    //MFS usage ( backward compatibility )

    //Deprecated
    fprintf(stderr,"\ndeprecated tools:\n");
    fprintf(stderr,"\t%srgetgoal = %sgetgoal -r\n", MFS_PREF, MFS_PREF );
    fprintf(stderr,"\t%srsetgoal = %ssetgoal -r\n", MFS_PREF, MFS_PREF );
    fprintf(stderr,"\t%srgettrashtime = %sgettreshtime -r\n", MFS_PREF, MFS_PREF );
    fprintf(stderr,"\t%srsettrashtime = %ssettreshtime -r\n", MFS_PREF, MFS_PREF );
}


