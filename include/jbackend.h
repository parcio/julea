/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Michael Kuhn
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

#ifndef H_BACKEND
#define H_BACKEND

#include <bson.h>

#include <jitem.h>

struct JBackendItem
{
	gchar* path;
	gpointer user_data;
};

typedef struct JBackendItem JBackendItem;

enum JBackendType
{
	J_BACKEND_TYPE_DATA,
	J_BACKEND_TYPE_META
};

typedef enum JBackendType JBackendType;

struct JBackend
{
	JBackendType type;

	union
	{
		struct
		{
			gboolean (*init) (gchar const*);
			void (*fini) (void);

			gpointer (*thread_init) (void);
			void (*thread_fini) (gpointer);

			gboolean (*create) (JBackendItem*, gchar const*, gpointer);
			gboolean (*delete) (JBackendItem*, gpointer);

			gboolean (*open) (JBackendItem*, gchar const*, gpointer);
			gboolean (*close) (JBackendItem*, gpointer);

			gboolean (*status) (JBackendItem*, JItemStatusFlags, gint64*, guint64*, gpointer);
			gboolean (*sync) (JBackendItem*, gpointer);

			gboolean (*read) (JBackendItem*, gpointer, guint64, guint64, guint64*, gpointer);
			gboolean (*write) (JBackendItem*, gconstpointer, guint64, guint64, guint64*, gpointer);
		}
		data;

		struct
		{
			gboolean (*init) (gchar const*);
			void (*fini) (void);

			gpointer (*thread_init) (void);
			void (*thread_fini) (gpointer);

			gboolean (*batch_start) (gchar const*, gpointer*);
			gboolean (*batch_execute) (gpointer);

			gboolean (*create) (gchar const*, bson_t const*, gpointer);
			gboolean (*delete) (gchar const*, gpointer);

			gboolean (*get) (gchar const*, gchar const*, bson_t*);

			gboolean (*get_all) (gchar const*, gpointer*);
			gboolean (*get_by_value) (gchar const*, bson_t const*, gpointer*);
			gboolean (*iterate) (gpointer, bson_t const**);
		}
		meta;
	}
	u;
};

typedef struct JBackend JBackend;

JBackend* backend_info (JBackendType);

#endif
