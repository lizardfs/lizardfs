#ifndef LIZARDFS_MFSTOOLS_GETEATTR_H_
#define LIZARDFS_MFSTOOLS_GETEATTR_H_

#include "extendedattr.h"

class GetExtraAttrTool : public LizardCommand<ExtAttrParams> {

protected:
    virtual int executeCommand();
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
    //static const bool m_GetExtraAttrToolRegistered;
    GetExtraAttrTool();
    virtual void usage();
};

#endif //LIZARDFS_MFSTOOLS_GETEATTR_H_
