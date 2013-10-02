#ifndef LIZARDFS_MFSTOOLS_SETGOAL_H_
#define LIZARDFS_MFSTOOLS_SETGOAL_H_

#include "lizardtools.h"

struct SetGoalParams
{
    int argc;
    char** argv;
    uint8_t goal;
    int rflag;
    uint8_t smode;

    SetGoalParams() : argc(0), goal(0), rflag(0), smode(SMODE_SET) {
    }
};

class SetGoalTool : public LizardCommand<SetGoalParams> {

protected:
    virtual int executeCommand();
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
    //static const bool m_SetGoalToolRegistered;
    SetGoalTool();
    SetGoalTool(const std::string& pToolName, const bool& pDeprecated);
    virtual void usage();
};

#endif //LIZARDFS_MFSTOOLS_SETGOAL_H_
