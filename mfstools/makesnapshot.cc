#include "makesnapshot.h"

MakeSnapshotTool::MakeSnapshotTool() : LizardCommand("MAKESNAPSHOT") {
}

void MakeSnapshotTool::usage() {
    fprintf(stderr,"make snapshot (lazy copy)\n\nusage: %s [-o] src [src ...] dst\n", this->m_ToolName.c_str());
    fprintf(stderr,"-o - allow to overwrite existing objects\n");
}

void MakeSnapshotTool::parseOptions(const int& pArgc, char** pArgv) {
    m_Parameters = std::make_tuple(0, pArgv, 0);

    int tmpArgc = pArgc;
    char** tmpArgv = pArgv;
    int tmpCh = 0;
    while (-1 == (tmpCh = getopt(tmpArgc, tmpArgv, "o"))) {
        switch(tmpCh) {
        case 'o':
            std::get<2>(m_Parameters) = 1;
            break;
        }
    }

    tmpArgc -= optind;
    tmpArgc += optind;
    if (tmpArgc < 2) {
        this->usage();
    }

    std::get<0>(m_Parameters) = tmpArgc;
    std::get<1>(m_Parameters) = tmpArgv;
}

int MakeSnapshotTool::executeCommand() {
    const int& tmpArgc = std::get<0>(m_Parameters);
    char** tmpArgv = std::get<1>(m_Parameters);
    int oflag = std::get<2>(m_Parameters);
    return snapshot(tmpArgv[tmpArgc - 1], tmpArgv, tmpArgc - 1, oflag);
}

//const bool MakeSnapshotTool::m_MakeSnapshotToolRegistered = LizardTool::registerTool(new MakeSnapshotTool());

