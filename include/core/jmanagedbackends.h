/** \file JManagedBackends.h
 * \ingroup backend
 *
 * \copyright
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
 * \n
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * \n
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * \n
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#ifndef JULEA_MANAGED_BACKENDS_H
#define JULEA_MANAGED_BACKENDS_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly"
#endif

#include <glib.h>

G_BEGIN_DECLS

struct JConfiguration;
typedef struct JConfiguration JConfiguration;

struct JList;
typedef struct JList JList;

struct JBackend;
typedef struct JBackend JBackend;

/**
 * \defgroup JManagedBackends
 *
 * \todo rename to JManagedObjectBackends?
 *
 * A Collection of multiple object backends, which distributed the objects
 * controlled by a policy Module.
 *
 * @{
 **/

/**
 * \defgroup JStorageTier
 *
 * \todo evaluate characteristics
 
 * Collection of characteristics about a storage media, used for distribution
 * decisions by the policy. 
 *
 * \attention all values contained in this may only fixed values
 * and as this not measured!
 * 	
 * @{
 **/

struct JStorageTier;
typedef struct JStorageTier JStorageTier;

/** 
 * get estimated storage tier bandwidth in bytes per second
 *
 * \param[in] this a storage tier  
 *
 * \return mean of bandwidth in bytes per second
 **/
guint64 j_backend_storage_tier_get_bandwidth(const JStorageTier* this);

/** get estimated storage tier latency in us
 *
 * \param[in] this a storage tier
 *
 * \return mean of latency in us
 **/
guint64 j_backend_storage_tier_get_latency(const JStorageTier* this);

/** get estimated total capacity  of storage tier in bytes
 *
 * \param[in] this a storage tier
 *
 * \return storage tier capacity in bytes
 **/
guint64 j_backend_storage_tier_get_capacity(const JStorageTier* this);

/**
 * @}
 */

struct JManagedBackends;
typedef struct JManagedBackends JManagedBackends;

/**
 * Contains data about an ongoing access to a object. 
 **/
struct JManagedBackendScope;
typedef struct JManagedBackendScope JManagedBackendScope;

/**
 * Possible Access types to log for JObjectBackendPolicy
 * used to signal the usage of their counter part j_object_backend_ functions
 * \sa j_object_backend_policy_process_access
 **/
typedef enum
{
	J_OBJECT_ACCESS_WRITE,
	J_OBJECT_ACCESS_READ,
	J_OBJECT_ACCESS_DELETE,
	J_OBJECT_ACCESS_SYNC,
	J_OBJECT_ACCESS_STATUS
} JObjectBackendAccessTypes;

/**
 * Policy to distribute objects between object backends in a JManagedBackends.
 *
 * process_create, process_access and process_message are used
 * to signal the policy on different kinds of events.
 * The implementation of this functions should be minimalistic
 * to reduce latency for the access, because this functions must terminate
 * before the access can continued.
 *
 * process is executed in a separate thread to provide resources necessary
 * complex calculation and executing migrations.
 **/
typedef struct
{
	/**
	 * Policy initializer.
	 *
	 * \param[out] policy_data a pointer to store address of policy internal data
	 * \param[in]  args a list of strings passed from the configuration file
	 *                  which may used to parametrise the policy.
	 *                  The list is like the strings NULL terminated
	 * \param[in]  backends access to the object backend instances.
	 **/
	gboolean (*init)(gpointer* policy_data, gchar const*const* args, JManagedBackends* backends);

	/**
	 * \param[in] policy_data data at address assigned in init
	 *            \attention do not forget to free your data if needed
	 **/
	gboolean (*fini)(gpointer policy_data);

	/**
	 * Function used to signal object access to the policy.
	 *
	 * \attention This function is called in the same thread as the access is
	 *            executed, therefore this function should be
	 *            handled as fast as possible to reduce access latency.
	 *
	 * \param[inout] policy_data pointer usage depends on policy
	 * \param[in] namespace of the object accessed
	 * \param[in] path of the object accessed
	 * \param[in] obj_id continuous index which is for  the live time of a object unique
	 * \param[in] tier identifier for the storage tier
	 *            the object is currently stored on
     * \param[in] access type of access performed \sa JObjectBackendAccessType
	 * \param[in] data corresponding to the access type.
	 *            For more details see \ref JObjectBackendAccessDetails 
	 **/
	gboolean (*process_access)(gpointer policy_data, const gchar* namespace, const gchar* path, guint obj_id, guint tier, JObjectBackendAccessTypes access, gconstpointer data);

	/// short processing period to match new object to a storage tier
	/** \retval FALSE on error
	 * \sa j_backend_stack_get_tiers */
	/**
	 * Signals a new object creation to the policy, sets instal storage tier.
	 *
	 * \param[inout] policy_data pointer usage depends on policy
	 * \param[in] namespace of the object which will be created
	 * \param[in] path of the object which will be created
	 * \param[in] obj_id \ref process_access
	 * \param[out] storage_tier ID of storage tier to create object on
	 **/
	gboolean (*process_create)(gpointer policy_data, const gchar* namespace, const gchar* path, guint obj_id, guint* storage_tier);

	/**
	 * handle messages send to policy.
	 *
	 * \todo add reply functionality
	 *
	 * \param[inout] policy_data pointer usage depends on policy
	 * \param[in] type message type encoded as 0-terminated string
	 * \param[inout] data optional message data
	 * \param[in] size of data field
	 **/
	gboolean (*process_message)(gpointer policy_data, const gchar* type, gpointer data, guint length);

	/**
	 * Function use to pass a dedicated thread for migrations
	 * and calculations to policy.
	 **/
	gboolean (*process)(gpointer policy_data);

	gpointer data; ///< reference to data allocated in init
} JObjectBackendPolicy;

JObjectBackendPolicy* backend_policy_info(void);

/**
 * \defgroup JObjectBackendAccessDetails 
 *
 * Structs used to pass additional data on a
 * JObjectBackendPolicy.process_access  call
 *
 * @{
 *
 **/

/**
 * Additional data for a read and write access
 **/
struct JObjectBackendRWAccess
{
	guint64 offset; ///< access offset
	guint64 length; ///< length of data to be written/read
};

/**
 * @}
 **/

/**
 * \param[in] config with information as about backend performances and policy to use
 * \param[in] object_backends a list instantiated object backends to use, in order as configured in config
 * \param[out] instance_ptr address of initialized JManagedBackends
 **/
gboolean j_backend_managed_init(JConfiguration* config, JList* object_backends, JManagedBackends** instance_ptr);

gboolean j_backend_managed_fini(JManagedBackends* this);

/**
 * Hands current thread to policy as processing resource
 * \sa JObjectBackendPolicy 
 *
 * \param[in] this JManagedBackends instance
 * \param[out] keep_running signals if the policy processing function should be called again after return
 *             \todo more useful if different calling pattern get defined
 **/
gboolean j_backend_managed_policy_process(JManagedBackends* this, gboolean* keep_running);

/**
 * Fetches backend where a given object is stored. Also blocks migration for that object.
 * Use j_backend_managed_object_open() to allow migration again. If the object not already exists
 * use j_backend_managed_object_create() to create it
 *
 * \param[in] this JManagedBackends instance
 * \param[in] namespace of the object
 * \param[in] path of the object
 * \param[out] backend where the object is stored
 * \param[out] scope object to track object access, e.g. needed to unlock the object later.
 **/
gboolean j_backend_managed_object_open(JManagedBackends* this, const gchar* namespace, const gchar* path, JBackend** backend, JManagedBackendScope** scope);

/**
 * Creates a object placed on a backend defined by the current policy.
 *
 * \param[in] this JManagedBackends instance
 * \param[in] namespace  of the object
 * \param[in] path of the object
 * \param[out] backend where the object should be created
 * \param[out] scope \ref j_backend_managed_object_open()
 **/
gboolean j_backend_managed_object_create(JManagedBackends* this, const gchar* namespace, const gchar* path, JBackend** backend, JManagedBackendScope** scope);

/**
 * Removes access flag from object. Needed to re-enable migration, after blocking with j_backend_managed_object_open() or j_backend_object_open().
 **/
gboolean j_backend_managed_object_close(JManagedBackendScope* scope);

/**
 * Sends a message to the policy.
 * \todo add reply channel 
 *
 * \param[in] this JManagedBackends instance
 * \param[in] type message type encoded as 0-terminated string
 * \param[inout] data optional message data
 * \param[in] size of data in bytes
 **/
gboolean j_backend_managed_policy_message(JManagedBackends* this, const gchar* type, gpointer data, guint length);

/**
 * Get storage tier characteristics, the position in the array correspond to the tierID.
 * \attention \ref JStorageTier
 *
 * \param[in] this JManagedBackends instance
 * \param[inout] tiers array of storage tiers, set to NULL if you only interested in the length.
 * \param[out] length number of storages tiers and length of tiers, if not NULL
 **/
gboolean j_backend_managed_get_tiers(JManagedBackends* this, const JStorageTier* const** tiers, guint* length);

/**
 * Fetch tier the object is currently stored on.
 *
 * \param[in] this JManagedBackends instance
 * \param[in] namespace  of the object
 * \param[in] path of the object
 *
 * \return tier id of tier the object is stored on
 * \retval -1 on error
 **/
guint j_backend_managed_get_tier(JManagedBackends* this, const gchar* namespace, const gchar* path);

/**
 * Migrates an object from one tier to another.
 *
 * If the destination tier is equal the current tier, nothing happens.
 *
 * The call blocks until all resources for the migration ar available and the migration is finished.
 * For returning immediately if the resources are busy use j_backend_stack_migrate_object_if_free()
 *
 * \param[in] this JManagedBackends instance
 * \param[in] namespace of the object to migrate
 * \param[in] path of the object to migrate
 * \param[in] dest tier id of migration destination
 *
 * \todo add migration command working with object ids?
 * \todo add non blocking version + schedule queued accesses
 **/
gboolean j_backend_managed_object_migrate(JManagedBackends* this, const gchar* namespace, const gchar* path, guint dest);

/**
 * If resources a free migrates the specified object to the destination tier, if not return.
 *
 * \retval FALSE on error or if resources are busy
 **/
gboolean j_backend_managed_object_migrate_if_free(JManagedBackends* this, const gchar* namespace, const gchar* path, guint dest);

/**
 * Stops all migrations at all backends, used for maintenance.
 * use j_backend_stack_unlock() to re-enable migration
 *
 * \attention changing object locations or editing them might crashes the policy!
 *
 * \param[in] this JManagedBackends instance
 * \param[inout] address to backends array of backends managed, set to NULL if not needed
 * \param[out] length
 * \todo remove for simpler code?
 **/
gboolean j_backend_managed_lock(JManagedBackends* this, JBackend*** backends, guint* length);

/**
 * returns backend management to JManagedBackends
 **/
gboolean j_backend_managed_unlock(JManagedBackends* this);

/**
 * Iterate through all existing object paths in a namespace.
 *
 * \param[in] this JManagedBackends instance
 * \param[in] namespace of objects to iterate
 * \param[out] iterator used with j_backend_stack_iterate() to access object paths
 *
 * \sa j_backend_stack_iterate(), j_backend_managed_get_by_prefix()
 **/
gboolean j_backend_managed_get_all(JManagedBackends* this, gchar const* namespace, gpointer* iterator);


/**
 * Iterate all object paths in a namespace with a given prefix
 *
 * \param[in] this JManagedBackends instance
 * \param[in] namespace of objects to iterate
 * \param[in] prefix of objects to iterate
 * \param[out] iterator used with j_backend_stack_iterate() to access object paths
 *
 * \sa j_backend_managed_get_all(), _backend_managed_iterate()
 **/
gboolean j_backend_managed_get_by_prefix(JManagedBackends* this, gchar const* namespace, gchar const* prefix, gpointer* iterator);

/**
 * Advance iterator to get the next path.
 *
 * \attention Objects added or removed while iterating may invalidates the iterator. This depends on the used kv-store-backend for the JManagedBackends
 *
 * \param[in] this JManagedBackends instance
 * \param[inout] iterator 
 * \param[out] path of next object
 *
 * \sa j_backend_managed_get_all(), j_backend_managed_get_by_prefix()
 **/
gboolean j_backend_managed_iterate(JManagedBackends* this, gpointer iterator, gchar const** path);

/**
 * @}
 **/

G_END_DECLS

#endif
