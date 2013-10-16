#ifndef LIZARDLINK_H
#define LIZARDLINK_H
#include "lizardtools.h"

class LizardLinkTool : public LizardCommand<std::pair<int, char**>> {

protected:
   virtual void parseOptions(const int& pArgc, char** pArgv);
   virtual int executeCommand();

public:
    LizardLinkTool();
    virtual void usage();
};

#endif // LIZARDLINK_H
