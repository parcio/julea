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


//libfabric includes
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>


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
		struct
		{
			size_t size;      			/* max entries on event queue */
			guint64 flags;     			/* operation flags */
			enum fi_wait_obj	wait_obj;  	/* used wait object */
			gint signaling_vector; 	/* interrupt affinity */
		} eq_attr;

		/**
		* Completion queue config
		*/
		struct
		{
			size_t size;      							/* max entries on completion queue, 0 indicates providers choice */
			guint64 flags;     							/* operation flags */
			enum fi_cq_format format;    		/* format for completion entries */
			enum fi_wait_obj wait_obj;  		/* used wait object */
			int signaling_vector; 					/* interrupt affinity */
			enum fi_cq_wait_cond wait_cond; /* additional wait condition */
		} cq_attr;

		/**
		*	Config Parameter for get_info
		*/
		struct
		{
			int version; 					/* libfabric versioning */
			gchar* node; 					/* user specified target node, format specified by info addr_format field */
			gchar* service;				/* user specified port represented as string */
			guint64 server_flags;	/* flags for fi_getinfo */
			guint64 client_flags;

			/**
			*fi_info config parameters
			*/
			struct
			{
				guint64 caps;									/* user specified provider capabilities */
				guint64 mode;									/* operational modes requested for providers */
				guint32 addr_format;					/* specifies the format for the adress input */
				gchar* prov_name;							/* user requested provider */
				enum fi_threading threading; 	/* user requested threading model */
				guint64 op_flags;							/* Transmit context flags */
			} info;
		} get_info;

		//TODO expand get_info if necessary (most likely for non-socket based communication)

	} libfabric;

	/**
	 * The reference count.
	 */
	gint ref_count;
};


gboolean
check_modes_validity(guint64 mode);

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
	* libfabric variables
	*/
	/* event queue variables */
	size_t eq_size;      								/* max entries on event queue */
	guint64 eq_flags;     							/* event queue operation flags */
	enum fi_wait_obj	eq_wait_obj;  		/* used wait object on event queue */
	gint eq_signaling_vector; 					/* event queue interrupt affinity */
	/* completion queue variables */
	size_t cq_size;      								/* max entries on completion queue, 0 indicates providers choice */
	guint64 cq_flags;     							/* completion queue operation flags */
	enum fi_cq_format cq_format;    		/* completion queue format for completion entries */
	enum fi_wait_obj cq_wait_obj;  			/* completion queue used wait object */
	gint cq_signaling_vector; 					/* completion queue  interrupt affinity */
	enum fi_cq_wait_cond cq_wait_cond; 	/* completion queue additional wait condition */
	/* getinfo variables */
	int version; 						/* libfabric versioning */
	gchar* node; 						/* user specified target node, format specified by info addr_format field */
	gchar* service;					/* port represented as string */
	guint64 client_flags;		/* flags for fi_getinfo */
	guint64 server_flags;
	guint64 caps;									/* user specified provider capabilities */
	guint64 mode;									/* operational modes requested for providers */
	guint32 addr_format;					/* specifies the format for the adress input */
	gchar* prov_name;							/* user requested provider */
	enum fi_threading threading; 	/* user requested threading model */ //TODO make it writeable for user
	guint64 op_flags;							/* Transmit context flags */


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
	mode = g_key_file_get_uint64(key_file, "libfabric", "modes", NULL);
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
			|| cq_size <= 0
			|| eq_size <= 0
			|| check_caps_validity(caps)
			|| check_modes_validity(mode)
			|| check_prov_name_validity(prov_name))
	{
		//if failed free components
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
		//TODO add libfabric fields
		return NULL;
	}

	/**
	* set libfabric values for non-user config
	*/
	eq_flags = 0;
	eq_wait_obj =	FI_WAIT_MUTEX_COND;
	eq_signaling_vector = 0;

	cq_flags = 0;
	cq_format = FI_CQ_FORMAT_MSG;
	cq_wait_obj = FI_WAIT_MUTEX_COND;
	cq_signaling_vector = 0;
	cq_wait_cond = FI_CQ_COND_NONE;

	version = FI_VERSION(1, 5);
	service = g_strdup("4711");
	addr_format = FI_SOCKADDR_IN;
	threading = FI_THREAD_SAFE;
	op_flags = FI_COMPLETION;
	server_flags = FI_SOURCE;
	client_flags = FI_NUMERICHOST;

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
	configuration->libfabric.eq_attr.size = eq_size;
	configuration->libfabric.eq_attr.flags = eq_flags;
	configuration->libfabric.eq_attr.wait_obj = eq_wait_obj;
	configuration->libfabric.eq_attr.signaling_vector = eq_signaling_vector;
	configuration->libfabric.cq_attr.size = cq_size;
	configuration->libfabric.cq_attr.flags = cq_flags;
	configuration->libfabric.cq_attr.format = cq_format;
	configuration->libfabric.cq_attr.wait_obj = cq_wait_obj;
	configuration->libfabric.cq_attr.signaling_vector = cq_signaling_vector;
	configuration->libfabric.cq_attr.wait_cond = cq_wait_cond;
	configuration->libfabric.get_info.version = version;
	configuration->libfabric.get_info.service = service;
	configuration->libfabric.get_info.server_flags = server_flags;
	configuration->libfabric.get_info.client_flags = client_flags;
	configuration->libfabric.get_info.info.caps = caps;
	configuration->libfabric.get_info.info.mode = mode;
	configuration->libfabric.get_info.info.addr_format = addr_format;
	configuration->libfabric.get_info.info.prov_name = prov_name;
	configuration->libfabric.get_info.info.threading = threading;
	configuration->libfabric.get_info.info.op_flags = op_flags;
	configuration->libfabric.get_info.node = node;


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
	if (configuration->cq_attr.size == 0)
	{
		configuration->cq_attr.size = 0;
	}
	*/

	//if not specified, use local machine as target
	if (configuration->libfabric.get_info.node == NULL)
	{
		configuration->libfabric.get_info.node = g_strdup("127.0.0.1");
	}

	//if neither a special provider is required NOR required capabilities are specified the sockets provider is used
	if (configuration->libfabric.get_info.info.prov_name == NULL && configuration->libfabric.get_info.info.caps == 0)
	{
		configuration->libfabric.get_info.info.prov_name = g_strdup("sockets");
	}

	/* 0 already is neutral element, here for explanation
	if (configuration->get_info.flags == 0)
	{
		configuration->get_info.flags = 0;
	}

	if (configuration->get_info.info.mode == 0)
	{
		configuration->get_info.info.mode = 0;
	}
	*/

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

		//TODO free JConfig libfabric fields

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




//TODO write geter function for libfabric values, add them as definition to jconfiguration.h include typecasts

//TODO write check functions

/**
 * @}
 **/
