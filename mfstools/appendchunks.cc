#include "appendchunks.h"

AppendChunksTool::AppendChunksTool() : LizardCommand("APPENDCHUNKS") {
}

void AppendChunksTool::usage() {
    fprintf(stderr,"append file chunks to another file. If destination file doesn't exist then it's created as empty file and then chunks are appended\n\nusage: %s dstfile name [name ...]\n", this->m_ToolName.c_str() );
}

void AppendChunksTool::parseOptions(const int& pArgc, char** pArgv) {
    std::get<0>(m_Parameters) = pArgc;
    std::get<1>(m_Parameters) = pArgv;

    while (getopt( std::get<0>(m_Parameters), std::get<1>(m_Parameters), "") != -1);
    std::get<0>(m_Parameters) -= optind;
    std::get<1>(m_Parameters) += optind;

    if (std::get<0>(m_Parameters) <= 1) {
        this->usage();
    }

    char* appendfname = std::get<1>(m_Parameters)[0];
    int i = open(appendfname, O_RDWR | O_CREAT, 0666);
    if (i < 0) {
        fprintf(stderr, "can't create/open file: %s\n", appendfname );
        std::get<2>(m_Parameters) = 1;
        return;
    }
    close(i);
    --std::get<0>(m_Parameters);
    ++std::get<1>(m_Parameters);
}

int AppendChunksTool::executeCommand() {
    if (1 == std::get<2>(m_Parameters)) return 1;

    int status = 0;
    while(get<0>(m_Parameters) > 0) {
        char* appendfname = (std::get<1>(m_Parameters) -1)[0];
        if (append_file(appendfname, *std::get<1>(m_Parameters)) < 0) {
            status = 1;
        }
        --std::get<0>(m_Parameters);
        ++std::get<1>(m_Parameters);
    }
    return status;
}

//const bool AppendChunksTool::m_AppendChunksToolRegistered = LizardTool::registerTool(new AppendChunksTool());
