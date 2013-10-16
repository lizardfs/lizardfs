#include "fileinfo.h"

FileInfoTool::FileInfoTool() : LizardCommand("FILEINFO") {
}

//parsing don't needed


void FileInfoTool::usage() {
    fprintf(stderr,"show files info (shows detailed info of each file chunk)\n\nusage: %s name [name ...]\n", this->m_ToolName.c_str() );
}

int FileInfoTool::executeCommand() {
    int status = 0;
    while(m_Parameters.first > 0) {
        if (file_info(*m_Parameters.second) <0 ) {
            status = 1;
        }
        --m_Parameters.first;
        ++m_Parameters.second;
    }
    return status;
}

//const bool FileInfoTool::m_FileInfoToolRegistered = LizardTool::registerTool(new FileInfoTool());

