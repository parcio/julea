/*
 * Copyright (c) 2017 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#ifndef H_BACKEND
#define H_BACKEND

#include <jitem.h>

struct JBackendItem
{
	gchar* path;
	gpointer user_data;
};

typedef struct JBackendItem JBackendItem;

enum JBackendComponent
{
	J_BACKEND_COMPONENT_CLIENT,
	J_BACKEND_COMPONENT_SERVER
};

typedef enum JBackendComponent JBackendComponent;

enum JBackendType
{
	J_BACKEND_TYPE_DATA,
	J_BACKEND_TYPE_META
};

typedef enum JBackendType JBackendType;

struct JBackend
{
	JBackendComponent component;
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
		}
		meta;
	}
	u;
};

typedef struct JBackend JBackend;

JBackend* backend_info (JBackendComponent, JBackendType);

#endif
