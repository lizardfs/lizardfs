#ifndef LIZARDFS_MFSTOOLS_GETTRASHTIME_H_
#define LIZARDFS_MFSTOOLS_GETTRASHTIME_H_

#include "lizardtools.h"

class GetTrashTimeTool : public LizardCommand<ToolParams> {

protected:
    virtual int executeCommand();
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
   // static const bool m_GetTrashTimeToolRegistered;
    GetTrashTimeTool();
    GetTrashTimeTool(const std::string& pToolName, const bool& pDeprecated);
    virtual void usage();
};

#endif //LIZARDFS_MFSTOOLS_GETTRASHTIME_H_
