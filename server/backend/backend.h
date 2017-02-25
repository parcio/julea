/*
 * Copyright (c) 2010-2017 Michael Kuhn
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
#include <jtrace-internal.h>

struct JBackendItem
{
	gchar* path;
	gpointer user_data;
};

typedef struct JBackendItem JBackendItem;

gboolean (*jd_backend_init) (gchar const*) = NULL;
void (*jd_backend_fini) (void) = NULL;

gpointer (*jd_backend_thread_init) (void) = NULL;
void (*jd_backend_thread_fini) (gpointer) = NULL;

gboolean (*jd_backend_create) (JBackendItem*, gchar const*, gchar const*, gchar const*, gpointer) = NULL;
gboolean (*jd_backend_delete) (JBackendItem*, gpointer) = NULL;

gboolean (*jd_backend_open) (JBackendItem*, gchar const*, gchar const*, gchar const*, gpointer) = NULL;
gboolean (*jd_backend_close) (JBackendItem*, gpointer) = NULL;

gboolean (*jd_backend_status) (JBackendItem*, JItemStatusFlags, gint64*, guint64*, gpointer) = NULL;
gboolean (*jd_backend_sync) (JBackendItem*, gpointer) = NULL;

gboolean (*jd_backend_read) (JBackendItem*, gpointer, guint64, guint64, guint64*, gpointer) = NULL;
gboolean (*jd_backend_write) (JBackendItem*, gconstpointer, guint64, guint64, guint64*, gpointer) = NULL;

#endif
