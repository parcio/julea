/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2019 Michael Kuhn
 * Copyright (C) 2019 Benjamin Warnke
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

#ifndef JULEA_BACKEND_H
#define JULEA_BACKEND_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>
#include <gmodule.h>

#include <bson.h>

#include <core/jsemantics.h>
#include <core/jmessage.h>
#include <core/jerror.h>

G_BEGIN_DECLS

enum JBackendType
{
	J_BACKEND_TYPE_OBJECT,
	J_BACKEND_TYPE_KV,
	J_BACKEND_TYPE_DB,
};

typedef enum JBackendType JBackendType;

enum JBackendComponent
{
	J_BACKEND_COMPONENT_CLIENT = 1 << 0,
	J_BACKEND_COMPONENT_SERVER = 1 << 1
};

typedef enum JBackendComponent JBackendComponent;

//! This struct contains the required functions for the different backends
struct JBackend
{
	JBackendType type;
	JBackendComponent component;

	union
	{
		struct
		{
			gboolean (*backend_init) (gchar const*);
			void (*backend_fini) (void);

			gboolean (*backend_create) (gchar const*, gchar const*, gpointer*);
			gboolean (*backend_open) (gchar const*, gchar const*, gpointer*);

			gboolean (*backend_delete) (gpointer);
			gboolean (*backend_close) (gpointer);

			gboolean (*backend_status) (gpointer, gint64*, guint64*);
			gboolean (*backend_sync) (gpointer);

			gboolean (*backend_read) (gpointer, gpointer, guint64, guint64, guint64*);
			gboolean (*backend_write) (gpointer, gconstpointer, guint64, guint64, guint64*);
		}
		object;

		struct
		{
			gboolean (*backend_init) (gchar const*);
			void (*backend_fini) (void);

			gboolean (*backend_batch_start) (gchar const*, JSemanticsSafety, gpointer*);
			gboolean (*backend_batch_execute) (gpointer);

			gboolean (*backend_put) (gpointer, gchar const*, gconstpointer, guint32);
			gboolean (*backend_delete) (gpointer, gchar const*);
			gboolean (*backend_get) (gpointer, gchar const*, gpointer*, guint32*);

			gboolean (*backend_get_all) (gchar const*, gpointer*);
			gboolean (*backend_get_by_prefix) (gchar const*, gchar const*, gpointer*);
			gboolean (*backend_iterate) (gpointer, gchar const**, gconstpointer*, guint32*);
		}
		kv;

		struct
		{
			gboolean (*backend_init) (gchar const*);
			void (*backend_fini) (void);
			gboolean (*backend_batch_start) (gchar const* namespace, JSemanticsSafety safety, gpointer* batch, GError** error); //TODO check not null on module load
			gboolean (*backend_batch_execute) (gpointer batch, GError** error); //TODO check not null on module load
			/*!
create a schema in the db-backend
@param namespace [in] different usecases (e.g. "adios", "hdf5")
@param name [in] schema name to create (e.g. "files")
@param schema [in] the schema-structure to create. points to
 - an initialized bson containing "structure"
@verbatim
structure{
	"var_name1": var_type1 (int32),
	"var_name2": var_type2 (int32),
	"var_nameN": var_typeN (int32),
	"_indexes": [["var_name1", "var_name2"], ["var_name3"]],
}
@endverbatim
@return success
*/
			gboolean (*backend_schema_create) (gpointer batch, gchar const* name, bson_t const* schema, GError** error);
			/*!
obtains information about a schema in the db-backend
@param namespace [in] different usecases (e.g. "adios", "hdf5")
@param name [in] schema name to open (e.g. "files")
@param schema [out] the schema information initially points to
 - an allocated, but NOT initialized Bson
 - NULL
@verbatim
{
	"_id": (int64)
	"var_name1": var_type1 (int32),
	"var_name2": var_type2 (int32),
	"var_nameN": var_typeN (int32),
}
@endverbatim
@return success
*/
			gboolean (*backend_schema_get) (gpointer batch, gchar const* name, bson_t* schema, GError** error);
			/*!
delete a schema in the db-backend
@param namespace [in] different usecases (e.g. "adios", "hdf5")
@param name [in] schema name to delete (e.g. "files")
@return success
*/
			gboolean (*backend_schema_delete) (gpointer batch, gchar const* name, GError** error);
			/*!
insert data into a schema in the db-backend
@param namespace [in] different usecases (e.g. "adios", "hdf5")
@param name [in] schema name to delete (e.g. "files")
@param metadata [in] the data to insert. points to
 - an initialized bson_t containing "data"
@verbatim
data{
	"var_name1": value1,
	"var_name2": value2,
	"var_nameN": valueN,
}
@endverbatim
@return success
*/
			gboolean (*backend_insert) (gpointer batch, gchar const* name, bson_t const* metadata, GError** error);
			/*!
updates data in the db-backend
@param namespace [in] different usecases (e.g. "adios", "hdf5")
@param name [in] schema name to delete (e.g. "files")
@param selector [in] the selector to decide which data should be updated. points to
 - an initialized bson containing "selector_part_and"
@verbatim
selector_part: {
	"_mode": mode (int32)
	"0": {
		"_name": name1 (utf8),
		"_operator": op1 (int32),
		"_value": value1,
	},
	"4": selector_part (bson_t)
	"N": {
		"_name": name2 (utf8),
		"_operator": op2 (int32),
		"_value": value2,
	},
}
@endverbatim
@param metadata [in] the data to write. All undefined columns will be set to NULL
@verbatim
{
        "var_name1": value1,
        "var_name2": value2,
        "var_nameN": valueN,
}
@endverbatim
@return success
*/
			gboolean (*backend_update) (gpointer batch, gchar const* name, bson_t const* selector, bson_t const* metadata, GError** error);
			/*!
deletes data from the db-backend
@param namespace [in] different usecases (e.g. "adios", "hdf5")
@param name [in] schema name to delete (e.g. "files")
@param selector [in] the selector to decide which data should be updated. points to
 - an initialized bson containing "selector_part_and"
 - an empty bson
 - NULL
@verbatim
selector_part: {
        "_mode": mode (int32)
        "0": {
                "_name": name1 (utf8),
                "_operator": op1 (int32),
                "_value": value1,
        },
        "4": selector_part (bson_t)
        "N": {
                "_name": name2 (utf8),
                "_operator": op2 (int32),
                "_value": value2,
        },
}
@endverbatim
@return success
*/
			gboolean (*backend_delete) (gpointer batch, gchar const* name, bson_t const* selector, GError** error);
			/*!
creates an iterator for the db-backend
@param namespace [in] different usecases (e.g. "adios", "hdf5")
@param name [in] schema name to delete (e.g. "files")
@param selector [in] the selector to decide which data should be updated. points to
 - an initialized bson containing "selector_part_and"
 - an empty bson
 - NULL
@verbatim
selector_part: {
        "_mode": mode (int32)
        "0": {
                "_name": name1 (utf8),
                "_operator": op1 (int32),
                "_value": value1,
        },
        "4": selector_part (bson_t)
        "N": {
                "_name": name2 (utf8),
                "_operator": op2 (int32),
                "_value": value2,
        },
}
@endverbatim
@param iterator [out] the iterator which can be used later for backend_iterate
@return success
*/
			gboolean (*backend_query) (gpointer batch, gchar const* name, bson_t const* selector, gpointer* iterator, GError** error);
			/*!
obtains metadata from the backend
backend_iterate should be called until the returned value is NULL due to no more elements found - this allowes the backend to free potentially 
allocated caches.
@param iterator [inout] the iterator specifying the data to retrieve
@param metadata [out] the requested metadata initially points to
 - an initialized bson - ready to write a document
@verbatim
{
	"_id": value0,
	"var_name1": value1,
	"var_name2": value2,
	"var_nameN": valueN,
}
@endverbatim
@return success
*/
			gboolean (*backend_iterate) (gpointer iterator, bson_t* metadata, GError** error);
		}
		db;
	};
};
typedef struct JBackend JBackend;
/* following functions could be generalized to be used with all backends -->> */
enum JBackend_db_parameter_type
{
	J_DB_PARAM_TYPE_STR = 0,
	J_DB_PARAM_TYPE_BLOB,
	J_DB_PARAM_TYPE_BSON,
	J_DB_PARAM_TYPE_ERROR,
	_J_DB_PARAM_TYPE_COUNT,
};
typedef enum JBackend_db_parameter_type JBackend_db_parameter_type;
struct JBackend_db_operation
{
	union
	{ //only for temporary static storage
		struct
		{
			gboolean bson_initialized;
			bson_t bson;
		};
		struct
		{
			GError error;
			GError* error_ptr;
		};
	};
	union
	{
		gconstpointer ptr_const;
		gpointer ptr;
	};
	gint len; //of ptr data
	JBackend_db_parameter_type type;
};
typedef struct JBackend_db_operation JBackend_db_operation;
struct JBackend_db_operation_data;
typedef struct JBackend_db_operation_data JBackend_db_operation_data;
struct JBackend_db_operation_data
{
	gboolean (*backend_func) (JBackend* backend, gpointer batch, JBackend_db_operation_data* data);
	guint in_param_count;
	guint out_param_count;
	JBackend_db_operation in_param[20]; //into function
	//the last out parameter must be of type 'J_DB_PARAM_TYPE_ERROR'
	JBackend_db_operation out_param[20]; //retrieve from function
};
gboolean j_backend_db_message_from_data (JMessage* message, JBackend_db_operation* data, guint len);
gboolean j_backend_db_message_to_data (JMessage* message, JBackend_db_operation* data, guint len);
gboolean j_backend_db_message_to_data_static (JMessage* message, JBackend_db_operation* data, guint len);
/* previous functions could be generalized to be used with all backends <<-- */

JBackend* backend_info (void);

gboolean j_backend_load_client (gchar const*, gchar const*, JBackendType, GModule**, JBackend**);
gboolean j_backend_load_server (gchar const*, gchar const*, JBackendType, GModule**, JBackend**);

//object backend ->
gboolean j_backend_object_init (JBackend*, gchar const*);
void j_backend_object_fini (JBackend*);

gboolean j_backend_object_create (JBackend*, gchar const*, gchar const*, gpointer*);
gboolean j_backend_object_open (JBackend*, gchar const*, gchar const*, gpointer*);

gboolean j_backend_object_delete (JBackend*, gpointer);
gboolean j_backend_object_close (JBackend*, gpointer);

gboolean j_backend_object_status (JBackend*, gpointer, gint64*, guint64*);
gboolean j_backend_object_sync (JBackend*, gpointer);

gboolean j_backend_object_read (JBackend*, gpointer, gpointer, guint64, guint64, guint64*);
gboolean j_backend_object_write (JBackend*, gpointer, gconstpointer, guint64, guint64, guint64*);

//kv backend ->
gboolean j_backend_kv_init (JBackend*, gchar const*);
void j_backend_kv_fini (JBackend*);

gboolean j_backend_kv_batch_start (JBackend*, gchar const*, JSemanticsSafety, gpointer*);
gboolean j_backend_kv_batch_execute (JBackend*, gpointer);

gboolean j_backend_kv_put (JBackend*, gpointer, gchar const*, gconstpointer, guint32);
gboolean j_backend_kv_delete (JBackend*, gpointer, gchar const*);
gboolean j_backend_kv_get (JBackend*, gpointer, gchar const*, gpointer*, guint32*);

gboolean j_backend_kv_get_all (JBackend*, gchar const*, gpointer*);
gboolean j_backend_kv_get_by_prefix (JBackend*, gchar const*, gchar const*, gpointer*);
gboolean j_backend_kv_iterate (JBackend*, gpointer, gchar const**, gconstpointer*, guint32*);

//db backend ->

gboolean j_backend_db_init (JBackend*, gchar const*);
void j_backend_db_fini (JBackend*);
extern const JBackend_db_operation_data j_db_schema_create_params;
extern const JBackend_db_operation_data j_db_schema_get_params;
extern const JBackend_db_operation_data j_db_schema_delete_params;
extern const JBackend_db_operation_data j_db_insert_params;
extern const JBackend_db_operation_data j_db_update_params;
extern const JBackend_db_operation_data j_db_delete_params;
extern const JBackend_db_operation_data j_db_get_all_params;
G_END_DECLS

#endif
