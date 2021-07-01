#include <core/jnetwork.h>

#include <gio/gio.h>

/// Used to initialize common parts between different connection.
/** This will create the following libfabric resources:
 *  * the domain
 *  * a event queue
 *  * a rx and tx completion queue
 *  * the endpoint
 *
 *	Also it will bind them accordingly and enable the endpoint.
 *  \protected \memberof JConnection
 *  \retval FALSE if initialisation failed
 */
gboolean
j_connection_init(struct JConnection* instance);

/// Allocated and bind memory needed for message transfer.
/** 
 * \protected \memberof JConnection
 * \retval FALSE if allocation or binding failed
 */
gboolean
j_connection_create_memory_resources(struct JConnection* instance);

