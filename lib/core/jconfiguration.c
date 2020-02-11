/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <jconfiguration.h>

#include <jbackend.h>
#include <jtrace.h>

#include <glib/gprintf.h>




/**
 * \defgroup JConfiguration Configuration
 *
 * @{
 **/

/**
 * A configuration.
 */
struct JConfiguration
{
	struct
	{
		gchar** object;			/* The object servers */
		gchar** kv;					/* The kv servers */
		gchar** db;					/* the db servers */
		guint32 object_len;	/* The number of object servers */
		guint32 kv_len;			/* The number of kv servers */
		guint32 db_len; 		/* The number of db servers */
	}
	servers;

	/**
	 * The object configuration.
	 */
	struct
	{
		gchar* backend;		/* The backend */
		gchar* component;	/* The component */
		gchar* path;			/* The path */
	}
	object;

	/**
	 * The kv configuration.
	 */
	struct
	{
		gchar* backend; 	/* the backend*/
		gchar* component; /* the component */
		gchar* path; 			/* the path */
	}
	kv;

	guint64 max_operation_size;
	guint32 max_connections;
	guint64 stripe_size;

	/**
	 * The db configuration.
	 */
	struct
	{
		gchar* backend;		/* the backend */
		gchar* component;	/* The component. */
		gchar* path; 			/* the path*/
	} db;

	/**
	* libfabric configuration fields
	*/
	struct
	{
		/**
		* Event queue config
		*/
		struct fi_eq_attr eq_attr;

		/**
		* Completion queue config
		*/
		struct fi_cq_attr cq_attr;

		/**
		*	Config Parameter for get_info
		*/
		struct
		{
			gint version; 				/* libfabric versioning */
			gchar* node; 					/* user specified target node, format specified by info addr_format field */
			gchar* service;				/* user specified port represented as string */
			guint64 server_flags;	/* flags for fi_getinfo */
			guint64 client_flags;

			/**
			*fi_info config parameters
			*/
			struct fi_info* hints;
		} get_info;
	} libfabric;

	/**
	 * The reference count.
	 */
	gint ref_count;
};


gboolean
check_prov_name_validity(gchar* prov_name);

gboolean
check_caps_validity(guint64 caps);

/**
 * Returns the configuration.
 *
 * \return The configuration.
 */
JConfiguration*
j_configuration (void)
{
	static JConfiguration* configuration = NULL;

	if (g_atomic_pointer_get(&configuration) == NULL)
	{
		// FIXME never freed
		g_atomic_pointer_compare_and_exchange(&configuration, NULL, j_configuration_new());
	}

	return configuration;
}

/**
 * Creates a new configuration.
 *
 * \code
 * \endcode
 *
 * \return A new configuration. Should be freed with j_configuration_unref().
 **/
JConfiguration*
j_configuration_new (void)
{
	J_TRACE_FUNCTION(NULL);

	JConfiguration* configuration = NULL;
	GKeyFile* key_file;
	gchar* config_name = NULL;
	gchar const* env_path;
	gchar* path = NULL;
	gchar const* const* dirs;

	key_file = g_key_file_new();

	if ((env_path = g_getenv("JULEA_CONFIG")) != NULL)
	{
		if (g_path_is_absolute(env_path))
		{
			if (g_key_file_load_from_file(key_file, env_path, G_KEY_FILE_NONE, NULL))
			{
				configuration = j_configuration_new_for_data(key_file);
			}
			else
			{
				g_critical("Can not open configuration file %s.", env_path);
			}

			/* If we do not find the configuration file, stop searching. */
			goto out;
		}
		else
		{
			config_name = g_path_get_basename(env_path);
		}
	}

	if (config_name == NULL)
	{
		config_name = g_strdup("julea");
	}

	path = g_build_filename(g_get_user_config_dir(), "julea", config_name, NULL);

	if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
	{
		configuration = j_configuration_new_for_data(key_file);

		goto out;
	}

	g_free(path);

	dirs = g_get_system_config_dirs();

	for (guint i = 0; dirs[i] != NULL; i++)
	{
		path = g_build_filename(dirs[i], "julea", config_name, NULL);

		if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
		{
			configuration = j_configuration_new_for_data(key_file);

			goto out;
		}

		g_free(path);
	}

	path = NULL;

out:
	g_key_file_free(key_file);

	g_free(path);
	g_free(config_name);

	return configuration;
}

/**
 * Creates a new configuration for the given configuration data.
 *
 * \code
 * \endcode
 *
 * \param key_file The configuration data.
 *
 * \return A new configuration. Should be freed with j_configuration_unref().
 **/
JConfiguration*
j_configuration_new_for_data (GKeyFile* key_file)
{
	J_TRACE_FUNCTION(NULL);

	/**
	* Julea config variables
	*/
	JConfiguration* configuration;
	gchar** servers_object;
	gchar** servers_kv;
	gchar** servers_db;
	gchar* object_backend;
	gchar* object_component;
	gchar* object_path;
	gchar* kv_backend;
	gchar* kv_component;
	gchar* kv_path;
	gchar* db_backend;
	gchar* db_component;
	gchar* db_path;
	guint64 max_operation_size;
	guint32 max_connections;
	guint64 stripe_size;
	/**
	* libfabric variables, expand here if necessary
	*/
	struct fi_eq_attr eq_attr;
	int eq_size;
	struct fi_cq_attr cq_attr;
	int cq_size;
	int version; 						/* libfabric versioning */
	gchar* node; 						/* user specified target node, format specified by info addr_format field */
	gchar* service;					/* port represented as string */
	guint64 client_flags;		/* flags for fi_getinfo */
	guint64 server_flags;
	struct fi_info* hints;
	gchar* prov_name;       /* user requested provider */
	guint64 caps;						/* user requested capabilities */


	g_return_val_if_fail(key_file != NULL, FALSE);

	/**
	* read julea information from config-file
	*/
	max_operation_size = g_key_file_get_uint64(key_file, "core", "max-operation-size", NULL);
	max_connections = g_key_file_get_integer(key_file, "clients", "max-connections", NULL);
	stripe_size = g_key_file_get_uint64(key_file, "clients", "stripe-size", NULL);
	servers_object = g_key_file_get_string_list(key_file, "servers", "object", NULL, NULL);
	servers_kv = g_key_file_get_string_list(key_file, "servers", "kv", NULL, NULL);
	servers_db = g_key_file_get_string_list(key_file, "servers", "db", NULL, NULL);
	object_backend = g_key_file_get_string(key_file, "object", "backend", NULL);
	object_component = g_key_file_get_string(key_file, "object", "component", NULL);
	object_path = g_key_file_get_string(key_file, "object", "path", NULL);
	kv_backend = g_key_file_get_string(key_file, "kv", "backend", NULL);
	kv_component = g_key_file_get_string(key_file, "kv", "component", NULL);
	kv_path = g_key_file_get_string(key_file, "kv", "path", NULL);
	db_backend = g_key_file_get_string(key_file, "db", "backend", NULL);
	db_component = g_key_file_get_string(key_file, "db", "component", NULL);
	db_path = g_key_file_get_string(key_file, "db", "path", NULL);
	/**
	* read user specified libfabric information from config-file
	*/
	eq_size = (size_t) g_key_file_get_uint64(key_file, "eq", "size", NULL);
	cq_size = (size_t) g_key_file_get_uint64(key_file, "cq", "size", NULL);
	node = g_key_file_get_string(key_file, "libfabric", "node", NULL);
	prov_name = g_key_file_get_string(key_file, "libfabric", "provider", NULL);
	/*
	* 22 capabilities available in libfabric for bitmask, but doubtful that even 10 will be combined
	* 13 of 22 are primary
	* not all supported, only socket based communication supported at the moment
	*/
	caps = g_key_file_get_uint64(key_file, "capabilities", "cap0", NULL) |
				 g_key_file_get_uint64(key_file, "capabilities", "cap1", NULL) |
				 g_key_file_get_uint64(key_file, "capabilities", "cap2", NULL) |
				 g_key_file_get_uint64(key_file, "capabilities", "cap3", NULL) |
				 g_key_file_get_uint64(key_file, "capabilities", "cap4", NULL) |
				 g_key_file_get_uint64(key_file, "capabilities", "cap5", NULL) |
				 g_key_file_get_uint64(key_file, "capabilities", "cap6", NULL) |
 				 g_key_file_get_uint64(key_file, "capabilities", "cap7", NULL) |
 				 g_key_file_get_uint64(key_file, "capabilities", "cap8", NULL) |
 				 g_key_file_get_uint64(key_file, "capabilities", "cap9", NULL);

	/**
	* check if vital components are missing
	*/
	if (servers_object == NULL || servers_object[0] == NULL
	    || servers_kv == NULL || servers_kv[0] == NULL
	    || servers_db == NULL || servers_db[0] == NULL
	    || object_backend == NULL
	    || object_component == NULL
	    || object_path == NULL
	    || kv_backend == NULL
	    || kv_component == NULL
	    || kv_path == NULL
	    || db_backend == NULL
	    || db_component == NULL
	    || db_path == NULL
			|| cq_size < 0
			|| eq_size < 0
			|| !check_caps_validity(caps)
			|| !check_prov_name_validity(prov_name))
	{
		//if failed free read components
		g_free(db_backend);
		g_free(db_component);
		g_free(db_path);
		g_free(kv_backend);
		g_free(kv_component);
		g_free(kv_path);
		g_free(object_backend);
		g_free(object_component);
		g_free(object_path);
		g_strfreev(servers_object);
		g_strfreev(servers_kv);
		g_strfreev(servers_db);
		g_free(node);
		g_free(prov_name);
		g_critical("Failed to build config\n");
		return NULL;
	}

	/**
	* set libfabric values in tmp variables
	*/
	// set event queue attributes
	// size, flags, used wait object, signaling vector, optional wait set (if used as wait object)
	eq_attr.size = eq_size;
	eq_attr.flags = 0;
	eq_attr.wait_obj = FI_WAIT_MUTEX_COND;
	eq_attr.signaling_vector = 0;
	eq_attr.wait_set = NULL;

	// set completion queue attributes
	// size, flags, used format for msg, used wait object, signaling vector, additional optional wait condition, optional wait set (if used as wait object)
	cq_attr.size = cq_size;
	cq_attr.flags = 0;
	cq_attr.format = FI_CQ_FORMAT_MSG;
	cq_attr.wait_obj = FI_WAIT_MUTEX_COND;
	cq_attr.signaling_vector = 0;
	cq_attr.wait_cond = FI_CQ_COND_NONE;
	cq_attr.wait_set = NULL;

	// set information for fi_getinfo call
	version = FI_VERSION(1, 5);
	service = g_strdup("4711");
	server_flags = FI_SOURCE;
	client_flags = FI_NUMERICHOST;
	hints = fi_allocinfo();

	hints->caps = caps;
	hints->mode = 0;
	hints->addr_format = FI_SOCKADDR_IN;
	hints->fabric_attr->prov_name = g_strdup(prov_name);
	hints->domain_attr->threading = FI_THREAD_SAFE;
	hints->tx_attr->op_flags = FI_COMPLETION;
		/**
	* sets values in config
	*/
	configuration = g_slice_new(JConfiguration);
	configuration->servers.object = servers_object;
	configuration->servers.kv = servers_kv;
	configuration->servers.db = servers_db;
	configuration->servers.object_len = g_strv_length(servers_object);
	configuration->servers.kv_len = g_strv_length(servers_kv);
	configuration->servers.db_len = g_strv_length(servers_db);
	configuration->object.backend = object_backend;
	configuration->object.component = object_component;
	configuration->object.path = object_path;
	configuration->kv.backend = kv_backend;
	configuration->kv.component = kv_component;
	configuration->kv.path = kv_path;
	configuration->db.backend = db_backend;
	configuration->db.component = db_component;
	configuration->db.path = db_path;
	configuration->max_operation_size = max_operation_size;
	configuration->max_connections = max_connections;
	configuration->stripe_size = stripe_size;
	configuration->ref_count = 1;
	//libfabric config
	configuration->libfabric.eq_attr = eq_attr;
	configuration->libfabric.cq_attr = cq_attr;
	configuration->libfabric.get_info.version = version;
	configuration->libfabric.get_info.node = node;
	configuration->libfabric.get_info.service = service;
	configuration->libfabric.get_info.server_flags = server_flags;
	configuration->libfabric.get_info.client_flags = client_flags;
	configuration->libfabric.get_info.hints = hints;


	/**
	* set default values for not specified values by user
	*/
	if (configuration->max_operation_size == 0)
	{
		configuration->max_operation_size = 8 * 1024 * 1024;
	}

	if (configuration->max_connections == 0)
	{
		configuration->max_connections = g_get_num_processors();
	}

	if (configuration->stripe_size == 0)
	{
		configuration->stripe_size = 4 * 1024 * 1024;
	}

	//libfabric defaults
	if (configuration->libfabric.eq_attr.size == 0)
	{
		configuration->libfabric.eq_attr.size = 10;
	}

	/* cq_attr == 0 means providers choice, thus redundant, but here for explanation
	*/

	//if not specified, use local machine as target
	if (configuration->libfabric.get_info.node == NULL)
	{
		configuration->libfabric.get_info.node = g_strdup("127.0.0.1");
	}

	//if neither a special provider is required NOR required capabilities are specified the sockets provider is used
	if (configuration->libfabric.get_info.hints->fabric_attr->prov_name == NULL && configuration->libfabric.get_info.hints->caps == 0)
	{
		//g_printf("\nNeither Capabilities nor Provider requested, sockets provider will be used\n");
		configuration->libfabric.get_info.hints->fabric_attr->prov_name = g_strdup("sockets");
	}

	//check contents
	if(&configuration->libfabric.eq_attr == NULL)
	{
		g_critical("eq_attr failed to comply");
	}
	if(&configuration->libfabric.cq_attr == NULL)
	{
		g_critical("cq_attr failed to comply");
	}

	return configuration;
}

/**
 * Increases a configuration's reference count.
 *
 * \code
 * JConfiguration* c;
 *
 * j_configuration_ref(c);
 * \endcode
 *
 * \param configuration A configuration.
 *
 * \return #configuration.
 **/
JConfiguration*
j_configuration_ref (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	g_atomic_int_inc(&(configuration->ref_count));

	return configuration;
}

/**
 * Decreases a configuration's reference count.
 * When the reference count reaches zero, frees the memory allocated for the configuration.
 *
 * \code
 * \endcode
 *
 * \param configuration A configuration.
 **/
void
j_configuration_unref (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(configuration != NULL);

	if (g_atomic_int_dec_and_test(&(configuration->ref_count)))
	{
		g_free(configuration->db.backend);
		g_free(configuration->db.component);
		g_free(configuration->db.path);

		g_free(configuration->kv.backend);
		g_free(configuration->kv.component);
		g_free(configuration->kv.path);

		g_free(configuration->object.backend);
		g_free(configuration->object.component);
		g_free(configuration->object.path);

		g_strfreev(configuration->servers.object);
		g_strfreev(configuration->servers.kv);
		g_strfreev(configuration->servers.db);

		g_free(configuration->libfabric.get_info.node);
		g_free(configuration->libfabric.get_info.service);

		fi_freeinfo(configuration->libfabric.get_info.hints);

		g_slice_free(JConfiguration, configuration);
	}
}

gchar const*
j_configuration_get_server (JConfiguration* configuration, JBackendType backend, guint32 index)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			g_return_val_if_fail(index < configuration->servers.object_len, NULL);
			return configuration->servers.object[index];
		case J_BACKEND_TYPE_KV:
			g_return_val_if_fail(index < configuration->servers.kv_len, NULL);
			return configuration->servers.kv[index];
		case J_BACKEND_TYPE_DB:
			g_return_val_if_fail(index < configuration->servers.db_len, NULL);
			return configuration->servers.db[index];
		default:
			g_assert_not_reached();
	}

	return NULL;
}

guint32
j_configuration_get_server_count (JConfiguration* configuration, JBackendType backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			return configuration->servers.object_len;
		case J_BACKEND_TYPE_KV:
			return configuration->servers.kv_len;
		case J_BACKEND_TYPE_DB:
			return configuration->servers.db_len;
		default:
			g_assert_not_reached();
	}

	return 0;
}

gchar const*
j_configuration_get_backend (JConfiguration* configuration, JBackendType backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			return configuration->object.backend;
		case J_BACKEND_TYPE_KV:
			return configuration->kv.backend;
		case J_BACKEND_TYPE_DB:
			return configuration->db.backend;
		default:
			g_assert_not_reached();
	}

	return NULL;
}

gchar const*
j_configuration_get_backend_component (JConfiguration* configuration, JBackendType backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			return configuration->object.component;
		case J_BACKEND_TYPE_KV:
			return configuration->kv.component;
		case J_BACKEND_TYPE_DB:
			return configuration->db.component;
		default:
			g_assert_not_reached();
	}

	return NULL;
}

gchar const*
j_configuration_get_backend_path (JConfiguration* configuration, JBackendType backend)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, NULL);

	switch (backend)
	{
		case J_BACKEND_TYPE_OBJECT:
			return configuration->object.path;
		case J_BACKEND_TYPE_KV:
			return configuration->kv.path;
		case J_BACKEND_TYPE_DB:
			return configuration->db.path;
		default:
			g_assert_not_reached();
	}

	return NULL;
}

guint64
j_configuration_get_max_operation_size (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->max_operation_size;
}

guint32
j_configuration_get_max_connections (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->max_connections;
}

guint64
j_configuration_get_stripe_size (JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->stripe_size;
}

// libfabric getters

struct fi_eq_attr*
j_configuration_get_fi_eq_attr(JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return &configuration->libfabric.eq_attr;
}

struct fi_cq_attr*
j_configuration_get_fi_cq_attr(JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return &configuration->libfabric.cq_attr;
}

int
j_configuration_get_fi_version(JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return (int) configuration->libfabric.get_info.version;
}

char const*
j_configuration_get_fi_node(JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->libfabric.get_info.node;
}

char const*
j_configuration_get_fi_service(JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->libfabric.get_info.service;
}

/**
* server: type = 0
* client: type = 1
*/
uint64_t
j_configuration_get_fi_flags(JConfiguration* configuration, int type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	switch(type)
	{
		case 0:
			return (uint64_t) configuration->libfabric.get_info.server_flags;
		case 1:
			return (uint64_t) configuration->libfabric.get_info.client_flags;
		default:
		g_assert_not_reached();
	}
	return -1;
}

struct fi_info*
j_configuration_fi_get_hints(JConfiguration* configuration)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->libfabric.get_info.hints;
}



gboolean
check_prov_name_validity(gchar* prov_name)
{
	gboolean ret;
	const gchar* available_provs[7];
	const gchar* libfabric_provs[14];

	if(prov_name == NULL)
	{
		return TRUE;
	}

	ret = FALSE;
	available_provs[0] = "gni";
	available_provs[1] = "psm";
	available_provs[2] = "psm2";
	available_provs[3] = "rxm";
	available_provs[4] = "sockets";
	available_provs[5] = "verbs";
	available_provs[6] = "bgq";

	libfabric_provs[0] = "gni";
	libfabric_provs[1] = "psm";
	libfabric_provs[2] = "psm2";
	libfabric_provs[3] = "rxm";
	libfabric_provs[4] = "sockets";
	libfabric_provs[5] = "tcp"; //should work, but added post 1.5
	libfabric_provs[6] = "udp";
	libfabric_provs[7] = "usnic";
	libfabric_provs[8] = "verbs";
	libfabric_provs[9] = "bgq";
	libfabric_provs[10] = "Network Direct"; //PERROR not correct libfabric name, as of 1.5 experimental, thus not in available. fi_netdir
	libfabric_provs[11] = "mlx";
	libfabric_provs[12] = "shm"; //added post 1.5
	libfabric_provs[13] = "efa"; //added post 1.5

	for(int n = 0; n < 14; n++)
	{
		if(g_strcmp0(prov_name, libfabric_provs[n]) == 0)
		{
			for(int i = 0; i < 7; i++)
			{
				if(g_strcmp0(prov_name, available_provs[i]) == 0)
				{
					g_printf("Suitable Provider requested.\n");
					ret = TRUE;
					goto end;
				}
			}
			g_critical("\nThe requested Provider is not supported by Julea-libfabric implementation.\n");
			goto end;
		}
	}
	g_critical("\nThe requested Provider is no libfabric Provider.\n");

	end:
	return ret;
}

/**
* Checks whether requested Capabilities are acceptable
* split into primary and secondary caps for readablitiy reasons.
*/
gboolean
check_caps_validity(guint64 caps)
{
	gboolean ret;
	uint64_t internal_caps;
	uint64_t available_caps;
	uint64_t libfabric_caps;
	uint64_t primary_caps;
	uint64_t secondary_caps;
	if(caps == 0)
	{
		return TRUE;
	}
	ret = FALSE;
	internal_caps = (uint64_t) caps;
	available_caps = FI_MSG 				| /* Endpoints support sending and receiving of Messages or Datagrams */
									 FI_SEND				|	/* Endpoints support message Data Transfers */
									 FI_RECV				| /* Endpoints support receiving message Data Transfers */
									 FI_LOCAL_COMM 	| /* Endpoints support local host communication */
									 FI_REMOTE_COMM;	/* Endpoints support remote nodes */
  primary_caps = FI_MSG 					|
								 FI_RMA						|
								 FI_TAGGED  			|
								 FI_ATOMIC  			|
								 FI_MULTICAST 		|
								 FI_NAMED_RX_CTX 	|
								 FI_DIRECTED_RECV |
								 FI_READ					|
								 FI_WRITE					|
								 FI_RECV					|
								 FI_SEND					|
								 FI_REMOTE_READ		|
								 FI_REMOTE_WRITE;
	secondary_caps = FI_MULTI_RECV  |
	 								 FI_SOURCE			|
									 FI_RMA_EVENT		|
									 FI_SHARED_AV		|
									 FI_TRIGGER			|
									 FI_FENCE				|
									 FI_LOCAL_COMM	|
									 FI_REMOTE_COMM |
									 FI_SOURCE_ERR;
	libfabric_caps = primary_caps 	|
									 secondary_caps;

	if((libfabric_caps & internal_caps) == internal_caps)
	{
		if((available_caps & internal_caps) == internal_caps)
		{
			g_printf("Capabilities accepted. Does not guarantee a chosen combination of capabilities to result in a valid Provider\n");
			ret = TRUE;
		}
		else
		{
			g_critical("\nRequested capabilities contain at least one capability not supported by Julea implementation.\n");
		}
	}
	else
	{
		g_critical("\nRequested capabilities contain at least one non libfabric capability.\n");
	}
	return ret;
}


/**
 * @}
 **/
