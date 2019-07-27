/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2019 Michael Kuhn
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

#ifndef JULEA_BACKEND_OPERATION_H
#define JULEA_BACKEND_OPERATION_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>
#include <gmodule.h>

#include <bson.h>

#include <core/jbackend.h>
#include <core/jmessage.h>

G_BEGIN_DECLS

enum JBackendOperationParamType
{
	J_BACKEND_OPERATION_PARAM_TYPE_STR = 0,
	J_BACKEND_OPERATION_PARAM_TYPE_BLOB,
	J_BACKEND_OPERATION_PARAM_TYPE_BSON,
	J_BACKEND_OPERATION_PARAM_TYPE_ERROR,
	_J_BACKEND_OPERATION_PARAM_TYPE_COUNT
};

typedef enum JBackendOperationParamType JBackendOperationParamType;

struct JBackendOperationParam
{
	// Only for temporary static storage
	union
	{
		struct
		{
			gboolean bson_initialized;
			bson_t bson;
		};
		struct
		{
			const gchar* error_quark_string;
			GError error;
			GError* error_ptr;
		};
	};
	union
	{
		gconstpointer ptr_const;
		gpointer ptr;
	};

	// Length of ptr data
	gint len;
	JBackendOperationParamType type;
};

typedef struct JBackendOperationParam JBackendOperationParam;

struct JBackendOperation
{
	gboolean (*backend_func) (struct JBackend*, gpointer, struct JBackendOperation*);
	guint in_param_count;
	guint out_param_count;
	// Input parameters
	JBackendOperationParam in_param[20];
	// Output parameters
	// The last out parameter must be of type 'J_BACKEND_OPERATION_PARAM_TYPE_ERROR'
	JBackendOperationParam out_param[20];
};

typedef struct JBackendOperation JBackendOperation;

G_END_DECLS

#include <core/jmessage.h>

G_BEGIN_DECLS

gboolean j_backend_operation_unwrap_db_schema_create (JBackend*, gpointer, JBackendOperation*);
gboolean j_backend_operation_unwrap_db_schema_get (JBackend*, gpointer, JBackendOperation*);
gboolean j_backend_operation_unwrap_db_schema_delete (JBackend*, gpointer, JBackendOperation*);
gboolean j_backend_operation_unwrap_db_insert (JBackend*, gpointer, JBackendOperation*);
gboolean j_backend_operation_unwrap_db_update (JBackend*, gpointer, JBackendOperation*);
gboolean j_backend_operation_unwrap_db_delete (JBackend*, gpointer, JBackendOperation*);
gboolean j_backend_operation_unwrap_db_query (JBackend*, gpointer, JBackendOperation*);

gboolean j_backend_operation_to_message (JMessage* message, JBackendOperationParam* data, guint len);
gboolean j_backend_operation_from_message (JMessage* message, JBackendOperationParam* data, guint len);
gboolean j_backend_operation_from_message_static (JMessage* message, JBackendOperationParam* data, guint len);

static const JBackendOperation j_backend_operation_db_schema_create = {
	.backend_func = j_backend_operation_unwrap_db_schema_create,
	.in_param_count = 3,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_ERROR,
		},
	},
};

static const JBackendOperation j_backend_operation_db_schema_get = {
	.backend_func = j_backend_operation_unwrap_db_schema_get,
	.in_param_count = 2,
	.out_param_count = 2,
	.in_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
	},
	.out_param = {
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_ERROR },
	},
};

static const JBackendOperation j_backend_operation_db_schema_delete = {
	.backend_func = j_backend_operation_unwrap_db_schema_delete,
	.in_param_count = 2,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
	},
	.out_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_ERROR },
	},
};

static const JBackendOperation j_backend_operation_db_insert = {
	.backend_func = j_backend_operation_unwrap_db_insert,
	.in_param_count = 3,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_ERROR },
	},
};

static const JBackendOperation j_backend_operation_db_update = {
	.backend_func = j_backend_operation_unwrap_db_update,
	.in_param_count = 4,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_ERROR },
	},
};

static const JBackendOperation j_backend_operation_db_delete = {
	.backend_func = j_backend_operation_unwrap_db_delete,
	.in_param_count = 3,
	.out_param_count = 1,
	.in_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_ERROR },
	},
};

static const JBackendOperation j_backend_operation_db_query = {
	.backend_func = j_backend_operation_unwrap_db_query,
	.in_param_count = 3,
	.out_param_count = 2,
	.in_param = {
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_STR },
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
	},
	.out_param = {
		{
			.type = J_BACKEND_OPERATION_PARAM_TYPE_BSON,
			.bson_initialized = TRUE,
		},
		{ .type = J_BACKEND_OPERATION_PARAM_TYPE_ERROR },
	},
};

G_END_DECLS

#endif
