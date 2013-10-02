#include "setquota.h"

SetQuotaTool::SetQuotaTool() : LizardCommand("SETQUOTA") {
}

void SetQuotaTool::usage() {
    fprintf(stderr,"set quota for given directory (directories)\n\nusage: %s [-nhH] [-iI inodes] [-lL length] [-sS size] [-rR realsize] dirname [dirname ...]\n", this->m_ToolName.c_str() );;
    print_numberformat_options();
    fprintf(stderr," -i/-I - set soft/hard limit for number of filesystem objects\n");
    fprintf(stderr," -l/-L - set soft/hard limit for sum of files lengths\n");
    fprintf(stderr," -s/-S - set soft/hard limit for sum of file sizes (chunk sizes)\n");
    fprintf(stderr," -r/-R - set soft/hard limit for estimated hdd usage (usually size multiplied by goal)\n");
    fprintf(stderr,"\nAll numbers can have decimal point and SI/IEC symbol prefix at the end\ndecimal (SI): (k - 10^3 , M - 10^6 , G - 10^9 , T - 10^12 , P - 10^15 , E - 10^18)\nbinary (IEC 60027): (Ki - 2^10 , Mi - 2^20 , Gi - 2^30 , Ti - 2^40 , Pi - 2^50 , Ei - 2^60 )\n");
}

void SetQuotaTool::parseOptions(const int& pArgc, char** pArgv) {
    m_Parameters.argc = pArgc;
    m_Parameters.argv = pArgv;

    if (getuid()) {
        fprintf(stderr,"only root can change quota\n");
        this->usage();
    }

    uint64_t v = 0;

    int ch = 0;
    while ((ch = getopt(m_Parameters.argc, m_Parameters.argv,"nhHi:I:l:L:s:S:r:R:")) != -1) {
        switch(ch) {
        case 'n':
            LizardTool::m_Humode=0;
            break;
        case 'h':
            LizardTool::m_Humode=1;
            break;
        case 'H':
            LizardTool::m_Humode=2;
            break;
        case 'i':
            if (my_get_number(optarg, &v,std::numeric_limits<uint32_t>::max(),0)<0) {
                fprintf(stderr,"bad inodes limit\n");
                this->usage();
            }
            if (m_Parameters.qflags & QUOTA_FLAG_SINODES) {
                fprintf(stderr,"'soft inodes' quota defined twice\n");
                this->usage();
            }
            m_Parameters.sinodes = v;
            m_Parameters.qflags |= QUOTA_FLAG_SINODES;
            break;
        case 'I':
            if (my_get_number(optarg,&v,std::numeric_limits<uint32_t>::max(),0)<0) {
                fprintf(stderr,"bad inodes limit\n");
                this->usage();
            }
            if (m_Parameters.qflags & QUOTA_FLAG_HINODES) {
                fprintf(stderr,"'hard inodes' q::::uota defined twice\n");
                this->usage();
            }
            m_Parameters.hinodes = v;
            m_Parameters.qflags |= QUOTA_FLAG_HINODES;
            break;
        case 'l':
            if (my_get_number(optarg, &v, std::numeric_limits<uint64_t>::max(), 1) <0 ) {
                fprintf(stderr,"bad length limit\n");
                this->usage();
            }
            if (m_Parameters.qflags & QUOTA_FLAG_SLENGTH) {
                fprintf(stderr,"'soft length' quota defined twice\n");
                this->usage();
            }
            m_Parameters.slength = v;
            m_Parameters.qflags |= QUOTA_FLAG_SLENGTH;
            break;
        case 'L':
            if (my_get_number(optarg, &v, std::numeric_limits<uint64_t>::max(), 1) <0 ) {
                fprintf(stderr,"bad length limit\n");
                this->usage();
            }
            if (m_Parameters.qflags & QUOTA_FLAG_HLENGTH) {
                fprintf(stderr,"'hard length' quota defined twice\n");
                this->usage();
            }
            m_Parameters.hlength = v;
            m_Parameters.qflags |= QUOTA_FLAG_HLENGTH;
            break;
        case 's':
            if (my_get_number(optarg, &v, std::numeric_limits<uint64_t>::max(), 1 ) < 0) {
                fprintf(stderr,"bad size limit\n");
                this->usage();
            }
            if (m_Parameters.qflags & QUOTA_FLAG_SSIZE) {
                fprintf(stderr,"'soft size' quota defined twice\n");
                this->usage();
            }
            m_Parameters.ssize = v;
            m_Parameters.qflags |= QUOTA_FLAG_SSIZE;
            break;
        case 'S':
            if (my_get_number(optarg, &v, std::numeric_limits<uint64_t>::max(), 1 ) < 0) {
                fprintf(stderr,"bad size limit\n");
                this->usage();
            }
            if (m_Parameters.qflags & QUOTA_FLAG_HSIZE) {
                fprintf(stderr,"'hard size' quota defined twice\n");
                this->usage();
            }
            m_Parameters.hsize = v;
            m_Parameters.qflags |= QUOTA_FLAG_HSIZE;
            break;
        case 'r':
            if (my_get_number(optarg, &v, std::numeric_limits<uint64_t>::max(), 1 ) < 0) {
                fprintf(stderr,"bad real size limit\n");
                this->usage();
            }
            if (m_Parameters.qflags & QUOTA_FLAG_SREALSIZE) {
                fprintf(stderr,"'soft realsize':::: quota defined twice\n");
                this->usage();
            }
            m_Parameters.srealsize = v;
            m_Parameters.qflags |= QUOTA_FLAG_SREALSIZE;
            break;
        case 'R':
            if (my_get_number(optarg, &v, std::numeric_limits<uint64_t>::max(),1) <0 ) {
                fprintf(stderr,"bad real size limit\n");
                this->usage();
            }
            if (m_Parameters.qflags & QUOTA_FLAG_HREALSIZE) {
                fprintf(stderr,"'hard realsize' quota defined twice\n");
                this->usage();
            }
            m_Parameters.hrealsize = v;
            m_Parameters.qflags |= QUOTA_FLAG_HREALSIZE;
            break;
        }
    }
    if (m_Parameters.qflags == 0) {
        fprintf(stderr,"quota options not defined\n");
        this->usage();
    }
    m_Parameters.argc -= optind;
    m_Parameters.argv += optind;
}

int SetQuotaTool::executeCommand() {
    int status = 0;
    while(m_Parameters.argc > 0) {
        if (quota_control(*m_Parameters.argv, 0, m_Parameters.qflags, m_Parameters.sinodes,
                           m_Parameters.slength, m_Parameters.ssize, m_Parameters.srealsize,
                           m_Parameters.hinodes, m_Parameters.hlength, m_Parameters.hsize,
                          m_Parameters.hrealsize) < 0) {
                status=1;
            }
        --m_Parameters.argc;
        ++m_Parameters.argv;
     }
    return status;
}

//const bool SetQuotaTool::m_SetQuotaToolRegistered = LizardTool::registerTool(new SetQuotaTool());



