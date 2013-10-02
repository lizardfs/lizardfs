#ifndef LIZARDFS_MFSTOOLS_HUMODEBASED_H_
#define LIZARDFS_MFSTOOLS_HUMODEBASED_H_

#include "lizardtools.h"

class HumodeBasedTool : public LizardCommand<std::pair<int, char**>> {

protected:
    virtual int executeCommand() = 0;
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
    virtual void usage() = 0;
    HumodeBasedTool(const std::string& pToolName);

};

#endif //LIZARDFS_MFSTOOLS_HUMODEBASED_H_
