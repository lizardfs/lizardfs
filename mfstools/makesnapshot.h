#ifndef LIZARDFS_MFSTOOLS_MAKESNAPSHOT_H_
#define LIZARDFS_MFSTOOLS_MAKESNAPSHOT_H_

#include "lizardtools.h"

class MakeSnapshotTool : public LizardCommand<tuple<int, char**, int>> {

protected:
    virtual int executeCommand();
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
   // static const bool m_MakeSnapshotToolRegistered;
    MakeSnapshotTool();
    virtual void usage();
};

#endif //LIZARDFS_MFSTOOLS_MAKESNAPSHOT_H_
