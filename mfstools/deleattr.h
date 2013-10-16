#ifndef LIZARDFS_MFSTOOLS_DELETEATTR_H_
#define LIZARDFS_MFSTOOLS_DELETEATTR_H_

#include "extendedattr.h"

class DeleteEAttrTool : public ExtendedAttributesTool {

protected:
        virtual int executeCommand();

    public:
        DeleteEAttrTool();
        virtual void usage();
//        static const bool m_DeleteEAttrToolRegistered;
};

#endif //LIZARDFS_MFSTOOLS_DELETEATTR_H_
