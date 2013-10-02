#include "dirinfo.h"

DirInfoTool::DirInfoTool() : HumodeBasedTool("DIRINFO") {
}

void DirInfoTool::usage() {
    fprintf(stderr,"show directories stats\n\nusage: %s [-nhH] name [name ...]\n", this->m_ToolName.c_str() );
    print_numberformat_options();
    fprintf(stderr,"\nMeaning of some not obvious output data:\n 'length' is just sum of files lengths\n 'size' is sum of chunks lengths\n 'realsize' is estimated hdd usage (usually size multiplied by current goal)\n");
}

int DirInfoTool::executeCommand() {
    int status = 0;
    while(m_Parameters.first > 0) {
        if (dir_info(*m_Parameters.second) < 0) {
            status=1;
        }
        --m_Parameters.first;
        ++m_Parameters.second;
    }
    return status;
}

//const bool DirInfoTool::m_DirInfoToolRegistered = LizardTool::registerTool(new DirInfoTool());
