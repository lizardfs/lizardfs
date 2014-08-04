#pragma once
#include "common/platform.h"

#include "mount/polonaise/setup.h"

/*! \brief Parse command line options and fill Setup struct.
 *
 * \param argc - command line argument count.
 * \param argv - command line arguments.
 * \param[out] setup - fill this setup struct with result of parsing.
 */
void parse_command_line(int argc, char** argv, Setup& setup);
