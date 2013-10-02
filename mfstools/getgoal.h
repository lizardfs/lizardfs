#ifndef LIZARDFS_MFSTOOLS_GETGOAL_H_
#define LIZARDFS_MFSTOOLS_GETGOAL_H_

#include "lizardtools.h"

class GetGoalTool : public LizardCommand<ToolParams> {

protected:
    virtual int executeCommand();
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
    GetGoalTool();
    GetGoalTool(const std::string& pToolName, const bool& pDeprecated);
    virtual void usage();
    //static const bool m_GetGoalToolRegistered;

};

#endif //LIZARDFS_MFSTOOLS_GETGOAL_H_
