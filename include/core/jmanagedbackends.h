/** \file jbackendstack.h
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
 
 * Collection of characteristics about a storage media, used for distributeion
 * decisions by the policy. 
 *
 * \attention all values contained in this may only fixed values
 * and as this not messuared!
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

/// 
/** get estimated storage tier latency in us
 *
 * \param[in] this a storage tier
 *
 * \return mean of latency in us
 **/
guint64 j_backend_storage_tier_get_latency(const JStorageTier* this);

/// 
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
	 * \param[in]  backends access to the object backend instances.
	 **/
	gboolean (*init)(gpointer* policy_data, const JList* args, JManagedBackends* backends);

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
struct JObjectBackendRWAccess {
	guint64 offset; ///< access offset 
	guint64 length; ///< length of data to be written/read 
};

/**
 * @}
 **/

gboolean j_backend_stack_init(
	JConfiguration* config,
	JList* object_backends, ///< [in] list of all initialized object backends
	JBackendStack** instance_ptr);
gboolean j_backend_stack_fini(JBackendStack* this);

/// gives policy cpu time
/// \todo how tell when this should be called?
gboolean j_backend_stack_policy_process(JBackendStack* this, gboolean* keep_running);

/// fetch backend from backendstack for given name & path
/** \public \memberof JBackendStack
 * if object not already exists use j_backend_stack_create()
 * \sa j_backend_stack_end */
gboolean j_backend_stack_begin(JBackendStack* this,
			       const gchar* namespace,
			       const gchar* path,
			       JBackend** backend,	  ///< [out] backend where object in initealy stored
			       JBackendStackScope** scope ///< [out] backend stack scope to close connection when finished
);


gboolean
j_backend_stack_create(JBackendStack* this,
		       const gchar* namespace,
		       const gchar* path,
		       JBackend** backend,
		       JBackendStackScope** scope);

/// closes backend connection opend with j_backend_stack_begin \todo redefine message
gboolean j_backend_stack_end(JBackendStackScope* scope);

/// send a message to the policy
/** \public \memberof JBackendStack
 * \retval FALSE on error
 * \sa JObjectBackendPolicy::process_message */
gboolean j_backend_stack_policy_message(JBackendStack* this,
					const gchar* type, ///< [in] message type encoded as 0-terminated string
					gpointer data,	   ///< [in,out] optional message data
					guint length	   ///< [in] size of message data
);

/// get storage tier characteristics (position = tierID)
/** \public \memberof JBackendStack
 * \retval FALSE on error */
gboolean j_backend_stack_get_tiers(JBackendStack* this,
				   const JStorageTier* const** tiers, ///< [out] array of storage tiers, set NULL if only the length is of interest
				   guint* length		      ///< [out] length of array
);

/// get current tier of object
/** \public \memberof JBackendStack
 * \return tier id, to look up tier data in results of j_backend_stack_get_tiers()
 * \retval -1 on error */
guint j_backend_stack_get_tier(JBackendStack* this,
			       const gchar* namespace, const gchar* path);

/// migrates an object from one tier to another
/** \public \memberof JBackendStack
 * if destination tier == current tier dose nothing
 * call will block until resources are free for migration, if you want only migarte if resources are free
 * use j_backend_stack_migrate_if_free()
 * \retval FALSE on error
 * \sa j_backend_stack_get_tiers
 * \todo maybe introduce object-ids for internal communication */
gboolean j_backend_stack_migrate(JBackendStack* this,
				 const gchar* namespace, ///< [in] of object to migrate
				 const gchar* path,	 ///< [in] of object to migrate
				 guint dest		 ///< [in] tier id of tier to migrate object to
);

/// migartes an object from on tier to another
/** \public \memberof JBackendStack
 * to wait until resources are free use j_backend_stack_migrate()
 * \retval FALSE if resource is bussy or on failure
 * \sa j_backend_stack_get_tiers
 */
gboolean j_backend_stack_migrate_if_free(JBackendStack* this,
					 const gchar* namespace, ///< [in] of object to migaret
					 const gchar* path,	 ///< [in] of object to migrate
					 guint dest		 ///< [in] tier id of tier to migrate object to
);

/// disable all migration actions and gives access to backends
/** \public \memberof JBackendStack
 * \deprecated
 * \retval FALSE on error
 * \sa j_backends_stack_unlock
 */
gboolean j_backend_stack_lock(JBackendStack* this,
			      JBackend*** backends, ///< [out] array of backends managed by stack, set to NULL if not interested
			      guint* length	    ///< [out] length of backends array, must differ from NULL if backends nq NULL
);

/// returns management to stack \deprecated
gboolean j_backend_stack_unlock(JBackendStack* this);

/// get iterator for all objects contained in namespace
/** \public \memberof JBackendStack
 * \retval FALSE on error
 * \sa j_backend_stack_iterate
 */
gboolean
j_backend_stack_get_all(JBackendStack* this, gchar const* namespace, gpointer* iterator);

/// get iterator for all objects contained in namespace with given prefix
/** \public \memberof JBackendStack
 * \retval FALSE on error
 * \sa j_backend_stack_iterate
 */
gboolean
j_backend_stack_get_by_prefix(JBackendStack* this, gchar const* namespace, gchar const * prefix, gpointer* iterator);

/// iterate through objects,
/** \public \memberof JBackendStack
 * \retval FALSE if no elements remain or on error
 * \sa j_backend_stack_get_all j_backend_stack_get_by_prefix
 */
gboolean
j_backend_stack_iterate(JBackendStack* this, gpointer iterator, gchar const** name);

/**
 * @}
 **/

G_END_DECLS

#endif
