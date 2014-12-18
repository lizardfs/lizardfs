#pragma once

#include "common/platform.h"

#include "common/server_connection.h"

/*! \brief this function reads password from stdin
 *
 */
std::string get_password();

/*! \brief  This function tries to register connection to master server using password
 *
 * \return status received from the master server
 */
uint8_t register_master_connection(ServerConnection& connection, std::string password);
