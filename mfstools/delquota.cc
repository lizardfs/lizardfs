#include "delquota.h"

DelQuotaTool::DelQuotaTool() : LizardCommand("DELQUOTA") {
}

void DelQuotaTool::usage() {
    fprintf(stderr,"delete quota for given directory (directories)\n\nusage: %s [-nhHailsrAILSR] dirname [dirname ...]\n", this->m_ToolName.c_str() );
    print_numberformat_options();
    fprintf(stderr," -i/-I - delete inodes soft/hard quota\n");
    fprintf(stderr," -l/-L - delete length soft/hard quota\n");
    fprintf(stderr," -s/-S - delete size soft/hard quota\n");
    fprintf(stderr," -r/-R - delete real size soft/hard quota\n");
    fprintf(stderr," -a/-A - delete all soft/hard quotas\n");
}

void DelQuotaTool::parseOptions(const int& pArgc, char** pArgv) {
    this->m_Parameters.argc = pArgc;
    this->m_Parameters.argv = pArgv;

    if (getuid()) {
        fprintf(stderr,"only root can change quota\n");
        this->usage();
    }
    int ch = 0;
    while (( ch = getopt(this->m_Parameters.argc, this->m_Parameters.argv, "nhHiIlLsSrRaA")) != -1) {
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
            if (m_Parameters.qflags & QUOTA_FLAG_SINODES) {
                fprintf(stderr,"'soft inodes' option given twice\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_SINODES;
            break;
        case 'I':
            if (m_Parameters.qflags & QUOTA_FLAG_HINODES) {
                fprintf(stderr,"'hard inodes' option given twice\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_HINODES;
            break;
        case 'l':
            if (m_Parameters.qflags & QUOTA_FLAG_SLENGTH) {
                fprintf(stderr,"'soft length' option given twice\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_SLENGTH;
            break;
        case 'L':
            if (m_Parameters.qflags & QUOTA_FLAG_HLENGTH) {
                fprintf(stderr,"'hard length' option given twice\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_HLENGTH;
            break;
        case 's':
            if (m_Parameters.qflags & QUOTA_FLAG_SSIZE) {
                fprintf(stderr,"'soft size' option given twice\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_SSIZE;
            break;
        case 'S':
            if (m_Parameters.qflags & QUOTA_FLAG_HSIZE) {
                fprintf(stderr,"'hard size' option given twice\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_HSIZE;
            break;
        case 'r':
            if (m_Parameters.qflags & QUOTA_FLAG_SREALSIZE) {
                fprintf(stderr,"'soft realsize' option given twice\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_SREALSIZE;
            break;
        case 'R':
            if (m_Parameters.qflags & QUOTA_FLAG_HREALSIZE) {
                fprintf(stderr,"'hard realsize' option given twice\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_HREALSIZE;
            break;
        case 'a':
            if (m_Parameters.qflags & QUOTA_FLAG_SALL) {
                fprintf(stderr,"'all soft quotas' defined together with other soft quota options\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_SALL;
            break;
        case 'A':
            if (m_Parameters.qflags & QUOTA_FLAG_HALL) {
                fprintf(stderr,"'all hard quotas' defined together with other hard quota options\n");
                this->usage();
            }
            m_Parameters.qflags |= QUOTA_FLAG_HALL;
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

int DelQuotaTool::executeCommand() {
    int status = 0;
    while(m_Parameters.argc > 0) {
        if (quota_control(*m_Parameters.argv, 1, m_Parameters.qflags, 0, 0, 0, 0, 0, 0, 0, 0) < 0 ) {
            status=1;
        }
        --m_Parameters.argc;
        ++m_Parameters.argv;
    }
    return status;
}

//const bool DelQuotaTool::m_DelQuotaToolRegistered = LizardTool::registerTool(new DelQuotaTool());
