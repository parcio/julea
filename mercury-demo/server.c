/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#include <unistd.h>
#include "j_mercury.h"


/**
 * Starts a server.
 *
 * \author Lars Thoms
 *
 * \code
 * \endcode
 *
 * \return Exit code. Should be zero, otherwise something did not worked well.
 **/
int main(void)
{
	/* Initialize connection */
	JMercuryConnection* connection;
	connection = j_mercury_init("cci+tcp://127.0.0.1:4242", TRUE);

	/* Register RPC */
	j_mercury_rpc_register(connection);

	/* Wait loop, because we use callback functions to react to incomming traffic */
	while(1)
	{
		sleep(1);
	}

	j_mercury_final(connection);

	return 0;
}
