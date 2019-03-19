/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2019 Michael Kuhn
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

enum JBackendType
{
	J_BACKEND_TYPE_OBJECT,
	J_BACKEND_TYPE_KV
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

			gboolean (*backend_batch_start) (gchar const*, JSemanticsSafety, gpointer*);
			gboolean (*backend_batch_execute) (gpointer);

			gboolean (*backend_put) (gpointer, gchar const*, bson_t const*);
			gboolean (*backend_delete) (gpointer, gchar const*);

			gboolean (*backend_get) (gchar const*, gchar const*, bson_t*);

			gboolean (*backend_get_all) (gchar const*, gpointer*);
			gboolean (*backend_get_by_prefix) (gchar const*, gchar const*, gpointer*);
			gboolean (*backend_iterate) (gpointer, bson_t*);
		}
		kv;
	};
};

typedef struct JBackend JBackend;

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

gboolean j_backend_kv_batch_start (JBackend*, gchar const*, JSemanticsSafety, gpointer*);
gboolean j_backend_kv_batch_execute (JBackend*, gpointer);

gboolean j_backend_kv_put (JBackend*, gpointer, gchar const*, bson_t const*);
gboolean j_backend_kv_delete (JBackend*, gpointer, gchar const*);

gboolean j_backend_kv_get (JBackend*, gchar const*, gchar const*, bson_t*);

gboolean j_backend_kv_get_all (JBackend*, gchar const*, gpointer*);
gboolean j_backend_kv_get_by_prefix (JBackend*, gchar const*, gchar const*, gpointer*);
gboolean j_backend_kv_iterate (JBackend*, gpointer, bson_t*);

G_END_DECLS

#endif
