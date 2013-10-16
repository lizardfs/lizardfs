#ifndef LIZARDFS_MFSTOOLS_APPENDCHUNKS_H_
#define LIZARDFS_MFSTOOLS_APPENDCHUNKS_H_

#include "lizardtools.h"

class AppendChunksTool : public LizardCommand<std::tuple<int, char**, int>> {

protected:
    virtual int executeCommand();
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
    //static const bool m_AppendChunksToolRegistered;
    AppendChunksTool();
    virtual void usage();
};

#endif //LIZARDFS_MFSTOOLS_APPENDCHUNKS_H_
