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

#ifndef JULEA_BACKEND_STACK_H
#define JULEA_BACKEND_STACK_H
#define JULEA_H ///< \todo remove debug
#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Onyl <julea.h> can be included directly"
#endif

#include <glib.h>

struct JConfiguration;
typedef struct JConfiguration JConfiguration;

struct JList;
typedef struct JList JList;

struct JBackend;
typedef struct JBackend JBackend;

struct JBackendStackScope;
typedef struct JBackendStackScope JBackendStackScope;

/// storage tier characteristics
/** \todo more detailed characteristics */
struct JStorageTier;
typedef struct JStorageTier JStorageTier;

/// get estimated storage tier bandwidth
/** \public \memberof JStorageTier
 * \return mean of bandwidth in bytes per second
 * */
guint64 j_backend_storage_tier_get_bandwidth(const JStorageTier* this);

/// get estimated storage tier latency
/** \public \memberof JStorageTier
 * \return mean of latency in us
 * */
guint64 j_backend_storage_tier_get_latency(const JStorageTier* this);

/// get estimated total capicity  of storage tier
/** \public \memberof JStorageTier */
guint64 j_backend_storage_tier_get_capacity(const JStorageTier* this);

/** \struct JBackendStack backend/jbackendstack.h
 * \ingroup backend
 * \brief Stack of all object backends to allow splitting data between based at a JObjectBackendPolicy
 */
struct JBackendStack;
typedef struct JBackendStack JBackendStack;

/// Possible Access types to log for JObjectBackendPolicy
/** \ingroup backend
 * used to signal the usage of their counter part j_object_backend_ functions
 * \sa j_object_backend_policy_process_access
 */
typedef enum
{
	J_OBJECT_ACCESS_WRITE,
	J_OBJECT_ACCESS_READ,
	J_OBJECT_ACCESS_DELETE,
	J_OBJECT_ACCESS_SYNC,
	J_OBJECT_ACCESS_STATUS
} JObjectBackendAccessTypes;

typedef struct
{
	gboolean (*init)(gpointer* this, const JList* args, JBackendStack* backendStack);
	gboolean (*fini)(gpointer this);
	/// short processing period to handle data
	gboolean (*process_access)(gpointer this,
				   const gchar* namespace,
				   const gchar* path,
				   guint obj_id, ///< [in] object index, used for easier recognise same object
				   guint tier,	 ///< [in] tier where object is currently on
				   JObjectBackendAccessTypes access,
				   gconstpointer data);
	/// long processing period for migration and large calculation
	/** \retval FALSE on error */
	gboolean (*process)(gpointer this);
	/// short processing period to match new object to a storage tier
	/** \retval FALSE on error
	 * \sa j_backend_stack_get_tiers */
	gboolean (*process_create)(gpointer this,
				   const gchar* namespace,
				   const gchar* path,
				   guint obj_id,      ///< [in] continious object index 
				   guint* storageTier ///< [out] storage tier ID to store new object
	);
	/// process policy custom message
	/** \retval FALSE on error */
	gboolean (*process_message)(gpointer this,
				    const gchar* type, ///< [in] message type encoded as 0-terminated string
				    gpointer data,     ///< [in,out] optional message data
				    guint length       ///< [in] size of data field
	);

	gpointer data;
} JObjectBackendPolicy;

JObjectBackendPolicy* backend_policy_info(void);

struct JObjectBackendRWAccess {
	guint64 offset;
	guint64 length;
};

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


#endif
