#include "extendedattr.h"

ExtendedAttributesTool::ExtendedAttributesTool(const std::string& pToolName)
    : LizardCommand(pToolName) {
}

void ExtendedAttributesTool::parseOptions(const int& pArgc, char** pArgv)
{
    m_Parameters = ExtAttrParams();

    m_Parameters.argc = int(pArgc);
    m_Parameters.argv = pArgv;

    int ch = 0;
    while ((ch=getopt( m_Parameters.argc, m_Parameters.argv,"rnhHf:")) != -1) {
        switch(ch) {
        case 'n':
            LizardTool::m_Humode = 0;
            break;
        case 'h':
            LizardTool::m_Humode = 1;
            break;
        case 'H':
            LizardTool::m_Humode = 2;
            break;
        case 'r':
            m_Parameters.rflag = 1;
            break;
        case 'f':
            int found = 0;
            for (int i = 0; found == 0 && i < EATTR_BITS; i++) {
                if (strcmp(optarg, eattrtab[i]) == 0) {
                    found = 1;
                    m_Parameters.eattr |= 1<<i;
                }
            }
            if (!found) {
                fprintf(stderr,"unknown flag\n");
                this->usage();
            }
            break;
        }
    }

    m_Parameters.argc -= optind;
    m_Parameters.argv += optind;
}
