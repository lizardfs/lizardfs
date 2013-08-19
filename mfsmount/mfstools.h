#ifndef _MFSTOOLS_H__
#define _MFSTOOLS_H__

#include <boost/assign/std/map.hpp>
#include <boost/assert.hpp>
#include <vector>
#include <string>
#include <map>
#include <set>
#include <algorithm>

using namespace std;
using namespace boost::assign;


class LizardTools
{
	public:
		const static char* LIZ_PREF;
		const static char* MFS_PREF;
		static bool m_StatLoaded;
		static bool loadStatic();
		static map< string, int > TOOLS_MAP;
		static map< string, string > TOOLS_DEPRECATED_MAP;
		const string m_ProgName;

		LizardTools( const char* pProgName );
		bool checkCommandName( const string& pCommandName );
		void errorInfo();
		void createFsLink();
		bool getFunctionEnum( pair< int, int>& pFunAttr );
		void usage( const int& pFunctionEnum );

	private:
		void usageInfo();
};


#endif