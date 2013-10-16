#ifndef LIZARDFS_MFSTOOLS_LIZARDFSTOOLS_H_
#define LIZARDFS_MFSTOOLS_LIZARDFSTOOLS_H_

#include <vector>
#include <string>
#include <map>
#include <set>
#include <algorithm>
#include <tuple>
#include <utility>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>

#include "MFSCommunication.h"

#  define UNUSED(x) UNUSED_ ## x __attribute__((__unused__))

using namespace std;

static const char* eattrtab[EATTR_BITS]={EATTR_STRINGS};
static const char* eattrdesc[EATTR_BITS]={EATTR_DESCRIPTIONS};

static inline void print_numberformat_options() {
    fprintf(stderr," -n - show numbers in plain format\n");
    fprintf(stderr," -h - \"human-readable\" numbers using base 2 prefixes (IEC 60027)\n");
    fprintf(stderr," -H - \"human-readable\" numbers using base 10 prefixes (SI)\n");
}

static inline void print_recursive_option() {
    fprintf(stderr," -r - do it recursively\n");
}

static inline void print_extra_attributes() {
    int j;
    fprintf(stderr,"\nattributes:\n");
    for (j=0 ; j<EATTR_BITS ; j++) {
        if (eattrtab[j][0]) {
            fprintf(stderr," %s - %s\n",eattrtab[j],eattrdesc[j]);
        }
    }
}

int snapshot(const char *dstname,char * const *srcnames,uint32_t srcelements,uint8_t canowerwrite);
int make_snapshot(const char *dstdir,const char *dstbase,const char *srcname,uint32_t srcinode,uint8_t canoverwrite);
int quota_control(const char *fname, uint8_t del, uint8_t qflags, uint32_t sinodes, uint64_t slength, uint64_t ssize, uint64_t srealsize, uint32_t hinodes, uint64_t hlength, uint64_t hsize, uint64_t hrealsize);
int set_goal(const char *fname, uint8_t goal, uint8_t mode);
void print_humanized_number(const char *format,uint64_t number,uint8_t flags);
void print_number(const char *prefix,const char *suffix,uint64_t number,uint8_t mode32,uint8_t bytesflag,uint8_t dflag);
int my_get_number(const char *str,uint64_t *ret,double max,uint8_t bytesflag);
int bsd_basename(const char *path,char *bname);
int bsd_dirname(const char *path,char *bname);
void dirname_inplace(char *path);
int master_register_old(int rfd);
int master_register(int rfd,uint32_t cuid);
int open_master_conn(const char *name,uint32_t *inode,mode_t *mode,uint8_t needsamedev,uint8_t needrwfs);
void close_master_conn(int err);
int check_file(const char* fname);
int get_goal(const char *fname,uint8_t mode);
int get_trashtime(const char *fname,uint8_t mode);
int get_eattr(const char *fname,uint8_t mode);
int set_trashtime(const char *fname,uint32_t trashtime,uint8_t mode);
int set_eattr(const char *fname,uint8_t eattr,uint8_t mode);
int file_info(const char *fname);
int append_file(const char *fname,const char *afname);
int dir_info(const char *fname);
int file_repair(const char *fname);

class LizardTool
{

    protected:
        static map<string, LizardTool*> LIZARD_TOOLS_MAP;

        void printUsageLine();
        const string m_ToolName;
        bool m_Deprecated;
        bool isDeprecated();

        virtual void parseOptions(const int& pArgc, char** pArgv) = 0;
        virtual int executeCommand() = 0;

    public:
        static uint8_t m_Humode;
        const static char* LIZ_PREF;
        const static char* MFS_PREF;
        bool static registerTool(LizardTool* pTool);
        bool static registerDeprecatedTool(const std::string& pToolName, LizardTool* pTool);
        string getToolName();
        LizardTool(const string& pToolName, const bool& pDeprecated = false);

        virtual ~LizardTool();
        void showHelpMsg();
        static void createFsLink(const char* pToolName);
        virtual void usage() = 0;
        int executeCommand(const int& pArgc, char** pArgv);
        static void getHumodeEnv( const char* pEnv );
        static map<string, LizardTool*>::iterator findTool(const string& pToolName);
        static map<string, LizardTool*>::iterator findEnd();

};

//T - tool name
//P - parameters initialized in parseOption
/*enum tool_id : short
{
    TOOL_1 = 0,
    TOOL_2,
    TOOL_3
};*/

template<class P = int, short I = 0>
class LizardCommand : public LizardTool
{
    protected:
        P m_Parameters;
        virtual int executeCommand() {
            return -1;
        }

        virtual void parseOptions(const int& UNUSED(pArgc), char** UNUSED(pArgv)) {
        }


    public:
        static bool m_ToolRegistered;
        LizardCommand(const std::string& pToolName, const bool& pDeprecated = false):
                        LizardTool(pToolName, pDeprecated) {
        }
        LizardCommand();
        virtual void usage() = 0;

};


struct ToolParams {
    int argc;
    char** argv;
    int rflag;
    ToolParams() : argc(0), rflag(0) {}
};

#endif //LIZARDFS_MFSTOOLS_LIZARDFSTOOLS_H_
