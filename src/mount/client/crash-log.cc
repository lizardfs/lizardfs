#include "crash-log.h"
#include <ctime>
#include <cstring>

void crashLog(const char *format, ...)
{
    FILE *f = fopen("/tmp/crashLog.txt", "a");

    if (f != NULL) {
        /* print message text */
        va_list args;
        va_start(args, format);
        time_t now = time(0);
        char *time_str = ctime(&now);
        time_str[strlen(time_str)-1] = '\0';
        time_str += 4;  // Removing day of the week
        fprintf(f, "%s -- ", time_str);
        vfprintf(f, format, args);
        va_end(args);
        fprintf(f, "\n");
        fclose(f);
    }
}
