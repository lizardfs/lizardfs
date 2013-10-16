#include "filerepair.h"

FileRepairTool::FileRepairTool() : HumodeBasedTool("FILEREPAIR") {
}

void FileRepairTool::usage() {
    fprintf(stderr,"repair given file. Use it with caution. It forces file to be readable, so it could erase (fill with zeros) file when chunkservers are not currently connected.\n\nusage: %s [-nhH] name [name ...]\n",this->m_ToolName.c_str() );
    print_numberformat_options();
}

int FileRepairTool::executeCommand() {
    int status = 0;
    while(m_Parameters.first > 0) {
        if (file_repair(*m_Parameters.second) < 0) {
            status=1;
        }
        --m_Parameters.first;
        ++m_Parameters.second;
    }
    return status;
}

//const bool FileRepairTool::m_FileRepairToolRegistered = LizardTool::registerTool( new FileRepairTool() );
