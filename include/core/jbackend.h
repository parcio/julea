/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2019 Michael Kuhn
 * Copyright (C) 2018-2019 Michael Straßberger
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

G_BEGIN_DECLS

#define J_BACKEND_BSON_ERROR j_backend_bson_error_quark()
#define J_BACKEND_DB_ERROR j_backend_db_error_quark()
#define J_BACKEND_SQL_ERROR j_backend_sql_error_quark()

// FIXME does it make sense to report these errors?
enum JBackendBSONError
{
	J_BACKEND_BSON_ERROR_BSON_APPEND_ARRAY_FAILED,
	J_BACKEND_BSON_ERROR_BSON_APPEND_DOCUMENT_FAILED,
	J_BACKEND_BSON_ERROR_BSON_APPEND_FAILED,
	J_BACKEND_BSON_ERROR_BSON_BUF_NULL,
	J_BACKEND_BSON_ERROR_BSON_COUNT_NULL,
	J_BACKEND_BSON_ERROR_BSON_HAS_FIELD_NULL,
	J_BACKEND_BSON_ERROR_BSON_INIT_FROM_JSON_FAILED,
	J_BACKEND_BSON_ERROR_BSON_JSON_NULL,
	J_BACKEND_BSON_ERROR_BSON_NAME_NULL,
	J_BACKEND_BSON_ERROR_BSON_NOT_ENOUGH_KEYS,
	J_BACKEND_BSON_ERROR_BSON_NULL,
	J_BACKEND_BSON_ERROR_BSON_VALUE_NULL,
	J_BACKEND_BSON_ERROR_ITER_EQUALS_NULL,
	J_BACKEND_BSON_ERROR_ITER_HAS_NEXT_NULL,
	J_BACKEND_BSON_ERROR_ITER_INIT,
	J_BACKEND_BSON_ERROR_ITER_INVALID_TYPE,
	J_BACKEND_BSON_ERROR_ITER_KEY_FOUND,
	J_BACKEND_BSON_ERROR_ITER_KEY_NOT_FOUND,
	J_BACKEND_BSON_ERROR_ITER_KEY_NULL,
	J_BACKEND_BSON_ERROR_ITER_NULL,
	J_BACKEND_BSON_ERROR_ITER_RECOURSE,
	J_BACKEND_BSON_ERROR_ITER_TYPE_NULL
};

typedef enum JBackendBSONError JBackendBSONError;

enum JBackendDBError
{
	J_BACKEND_DB_ERROR_COMPARATOR_INVALID,
	J_BACKEND_DB_ERROR_DB_TYPE_INVALID,
	J_BACKEND_DB_ERROR_ITERATOR_INVALID,
	J_BACKEND_DB_ERROR_ITERATOR_NO_MORE_ELEMENTS,
	J_BACKEND_DB_ERROR_NO_VARIABLE_SET,
	J_BACKEND_DB_ERROR_OPERATOR_INVALID,
	J_BACKEND_DB_ERROR_SCHEMA_EMPTY,
	J_BACKEND_DB_ERROR_SCHEMA_NOT_FOUND,
	J_BACKEND_DB_ERROR_SELECTOR_EMPTY,
	J_BACKEND_DB_ERROR_THREADING_ERROR,
	J_BACKEND_DB_ERROR_VARIABLE_NOT_FOUND
};

typedef enum JBackendDBError JBackendDBError;

enum JBackendSQLError
{
	J_BACKEND_SQL_ERROR_BIND,
	J_BACKEND_SQL_ERROR_CONSTRAINT,
	J_BACKEND_SQL_ERROR_FINALIZE,
	J_BACKEND_SQL_ERROR_INVALID_TYPE,
	J_BACKEND_SQL_ERROR_PREPARE,
	J_BACKEND_SQL_ERROR_RESET,
	J_BACKEND_SQL_ERROR_STEP
};

typedef enum JBackendSQLError JBackendSQLError;

enum JBackendType
{
	J_BACKEND_TYPE_OBJECT,
	J_BACKEND_TYPE_KV,
	J_BACKEND_TYPE_DB
};

typedef enum JBackendType JBackendType;

enum JBackendComponent
{
	J_BACKEND_COMPONENT_CLIENT = 1 << 0,
	J_BACKEND_COMPONENT_SERVER = 1 << 1
};

typedef enum JBackendComponent JBackendComponent;

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

			gboolean (*backend_batch_start) (gchar const*, JSemantics*, gpointer*);
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

			gboolean (*backend_batch_start) (gchar const*, JSemantics*, gpointer*, GError**);
			gboolean (*backend_batch_execute) (gpointer, GError**);

			/**
			* Create a schema
			*
			* \param[in] namespace Different use cases (e.g., "adios", "hdf5")
			* \param[in] name      Schema name to create (e.g., "files")
			* \param[in] schema    The schema structure to create. Points to:
			*                      - An initialized BSON containing "structure"
			* \code
			* structure
			* {
			*	"var_name1": var_type1 (int32),
			*	"var_name2": var_type2 (int32),
			*	"var_nameN": var_typeN (int32),
			*	"_indexes": [["var_name1", "var_name2"], ["var_name3"]]
			* }
			* \endcode
			*
			* \return TRUE on success, FALSE otherwise.
			**/
			gboolean (*backend_schema_create) (gpointer, gchar const*, bson_t const*, GError**);

			/**
			* Obtains information about a schema
			*
			* \param[in]  namespace Different use cases (e.g., "adios", "hdf5")
			* \param[in]  name      Schema name to open (e.g., "files")
			* \param[out] schema    The schema information initially points to:
			*                       - An allocated, uninitialized BSON
			*                       - NULL
			* \code
			* {
			*	"_id": id (int64),
			*	"var_name1": var_type1 (int32),
			*	"var_name2": var_type2 (int32),
			*	"var_nameN": var_typeN (int32)
			* }
			* \endcode
			*
			* \return TRUE on success, FALSE otherwise.
			**/
			gboolean (*backend_schema_get) (gpointer, gchar const*, bson_t*, GError**);

			/**
			* Delete a schema
			*
			* \param[in] namespace Different use cases (e.g., "adios", "hdf5")
			* \param[in] name      Schema name to delete (e.g., "files")
			*
			* \return TRUE on success, FALSE otherwise.
			**/
			gboolean (*backend_schema_delete) (gpointer, gchar const*, GError**);

			/**
			* Insert data into a schema
			*
			* \param[in] namespace Different use cases (e.g., "adios", "hdf5")
			* \param[in] name      Schema name to delete (e.g., "files")
			* \param[in] metadata  The data to insert. Points to:
			*                      - An initialized BSON containing "data"
			* \code
			* data
			* {
			*	"var_name1": value1,
			*	"var_name2": value2,
			*	"var_nameN": valueN,
			* }
			* \endcode
			*
			* \return TRUE on success, FALSE otherwise.
			**/
			gboolean (*backend_insert) (gpointer, gchar const*, bson_t const*, GError**);

			/**
			* Updates data
			*
			* \param[in] namespace Different use cases (e.g., "adios", "hdf5")
			* \param[in] name      Schema name to delete (e.g., "files")
			* \param[in] selector  The selector to decide which data should be updated. Points to:
			*                      - An initialized BSON containing "selector_part"
			* \code
			* selector_part
			* {
			*	"_mode": mode (int32)
			*	"0": {
			*		"_name": name1 (utf8),
			*		"_operator": op1 (int32),
			*		"_value": value1
			*	},
			*	"4": selector_part (document),
			*	"N": {
			*		"_name": name2 (utf8),
			*		"_operator": op2 (int32),
			*		"_value": value2
			*	}
			* }
			* \endcode
			*
			* \param[in] metadata  The data to write.
			* \code
			* {
			*	"var_name1": value1,
			*	"var_name2": value2,
			*	"var_nameN": valueN
			* }
			* \endcode
			*
			* \return TRUE on success, FALSE otherwise.
			**/
			gboolean (*backend_update) (gpointer, gchar const*, bson_t const*, bson_t const*, GError**);

			/**
			* Deletes data
			*
			* \param[in] namespace Different use cases (e.g., "adios", "hdf5")
			* \param[in] name      Schema name to delete (e.g., "files")
			* \param[in] selector  The selector to decide which data should be updated. Points to:
			*                      - An initialized bson containing "selector_part"
			*                      - An empty bson
			*                      - NULL
			* \code
			* selector_part
			* {
			*	"_mode": mode (int32)
			*	"0": {
			*		"_name": name1 (utf8),
			*		"_operator": op1 (int32),
			*		"_value": value1
			*	},
			*	"4": selector_part (document),
			*	"N": {
			*		"_name": name2 (utf8),
			*		"_operator": op2 (int32),
			*		"_value": value2
			*	}
			* }
			* \endcode
			*
			* \return TRUE on success, FALSE otherwise.
			**/
			gboolean (*backend_delete) (gpointer, gchar const*, bson_t const*, GError**);

			/**
			* Creates an iterator
			*
			* \param[in] namespace Different use cases (e.g., "adios", "hdf5")
			* \param[in] name      Schema name to delete (e.g., "files")
			* \param[in] selector  The selector to decide which data should be updated. Points to:
			*                      - An initialized BSON containing "selector_part"
			*                      - An empty BSON
			*                      - NULL
			* \code
			* selector_part
			* {
			*	"_mode": mode (int32),
			*	"0": {
			*		"_name": name1 (utf8),
			*		"_operator": op1 (int32),
			*		"_value": value1
			*	},
			*	"4": selector_part (document),
			*	"N": {
			*		"_name": name2 (utf8),
			*		"_operator": op2 (int32),
			*		"_value": value2
			*	},
			* }
			* \endcode
			* \param[out] iterator The iterator which can be used later for backend_iterate
			*
			* \return TRUE on success, FALSE otherwise.
			**/
			gboolean (*backend_query) (gpointer, gchar const*, bson_t const*, gpointer*, GError**);

			/**
			* Obtains metadata
			*
			* backend_iterate should be called until the returned value is NULL due to no more elements found.
			* This allows the backend to free potentially allocated caches.
			*
			* \param[in,out] iterator The iterator specifying the data to retrieve
			* \param[out]    metadata The requested metadata initially points to:
			*                         - An initialized BSON
			* \code
			* {
			* 	"_id": id,
			* 	"var_name1": value1,
			* 	"var_name2": value2,
			* 	"var_nameN": valueN
			* }
			* \endcode
			*
			* \return TRUE on success, FALSE otherwise.
			**/
			gboolean (*backend_iterate) (gpointer, bson_t*, GError**);
		}
		db;
	};
};

typedef struct JBackend JBackend;

GQuark j_backend_bson_error_quark(void);
GQuark j_backend_db_error_quark (void);
GQuark j_backend_sql_error_quark (void);

JBackend* backend_info (void);

gboolean j_backend_load_client (gchar const*, gchar const*, JBackendType, GModule**, JBackend**);
gboolean j_backend_load_server (gchar const*, gchar const*, JBackendType, GModule**, JBackend**);

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

gboolean j_backend_kv_init (JBackend*, gchar const*);
void j_backend_kv_fini (JBackend*);

gboolean j_backend_kv_batch_start (JBackend*, gchar const*, JSemantics*, gpointer*);
gboolean j_backend_kv_batch_execute (JBackend*, gpointer);

gboolean j_backend_kv_put (JBackend*, gpointer, gchar const*, gconstpointer, guint32);
gboolean j_backend_kv_delete (JBackend*, gpointer, gchar const*);
gboolean j_backend_kv_get (JBackend*, gpointer, gchar const*, gpointer*, guint32*);

gboolean j_backend_kv_get_all (JBackend*, gchar const*, gpointer*);
gboolean j_backend_kv_get_by_prefix (JBackend*, gchar const*, gchar const*, gpointer*);
gboolean j_backend_kv_iterate (JBackend*, gpointer, gchar const**, gconstpointer*, guint32*);

gboolean j_backend_db_init (JBackend*, gchar const*);
void j_backend_db_fini (JBackend*);

gboolean j_backend_db_batch_start (JBackend*, gchar const*, JSemantics*, gpointer*, GError**);
gboolean j_backend_db_batch_execute (JBackend*, gpointer, GError**);

gboolean j_backend_db_schema_create (JBackend*, gpointer, gchar const*, bson_t const*, GError**);
gboolean j_backend_db_schema_get (JBackend*, gpointer, gchar const*, bson_t*, GError**);
gboolean j_backend_db_schema_delete (JBackend*, gpointer, gchar const*, GError**);

gboolean j_backend_db_insert (JBackend*, gpointer, gchar const*, bson_t const*, GError**);
gboolean j_backend_db_update (JBackend*, gpointer, gchar const*, bson_t const*, bson_t const*, GError**);
gboolean j_backend_db_delete (JBackend*, gpointer, gchar const*, bson_t const*, GError**);

gboolean j_backend_db_query (JBackend*, gpointer, gchar const*, bson_t const*, gpointer*, GError**);
gboolean j_backend_db_iterate (JBackend*, gpointer, bson_t*, GError**);

G_END_DECLS

#endif
