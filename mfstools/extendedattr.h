#ifndef LIZARDFS_MFSTOOLS_EXTENDEDATTR_H_
#define LIZARDFS_MFSTOOLS_EXTENDEDATTR_H_

#include "lizardtools.h"

struct ExtAttrParams
{
    int argc;
    char **argv;
    uint8_t eattr;
    int rflag;

    ExtAttrParams() : argc(0), eattr(0), rflag(0) {}
};

class ExtendedAttributesTool : public LizardCommand<ExtAttrParams>
{
protected:
    virtual int executeCommand() = 0;
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
    virtual void usage() = 0;
    ExtendedAttributesTool(const std::string& pToolName);

};

#endif // LIZARDFS_MFSTOOLS_EXTENDEDATTR_H_
