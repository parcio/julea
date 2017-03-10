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

#ifndef H_BACKEND_INTERNAL
#define H_BACKEND_INTERNAL

#include <glib.h>
#include <gmodule.h>

#include <julea-internal.h>

#include <jbackend.h>

J_GNUC_INTERNAL GModule* j_backend_load_client (gchar const*, JBackendType, JBackend**);
J_GNUC_INTERNAL GModule* j_backend_load_server (gchar const*, JBackendType, JBackend**);

J_GNUC_INTERNAL gboolean j_backend_data_init (JBackend*, gchar const*);
J_GNUC_INTERNAL void j_backend_data_fini (JBackend*);

J_GNUC_INTERNAL gpointer j_backend_data_thread_init (JBackend*);
J_GNUC_INTERNAL void j_backend_data_thread_fini (JBackend*, gpointer);

J_GNUC_INTERNAL gboolean j_backend_data_create (JBackend*, JBackendItem*, gchar const*, gpointer);
J_GNUC_INTERNAL gboolean j_backend_data_delete (JBackend*, JBackendItem*, gpointer);

J_GNUC_INTERNAL gboolean j_backend_data_open (JBackend*, JBackendItem*, gchar const*, gpointer);
J_GNUC_INTERNAL gboolean j_backend_data_close (JBackend*, JBackendItem*, gpointer);

J_GNUC_INTERNAL gboolean j_backend_data_status (JBackend*, JBackendItem*, JItemStatusFlags, gint64*, guint64*, gpointer);
J_GNUC_INTERNAL gboolean j_backend_data_sync (JBackend*, JBackendItem*, gpointer);

J_GNUC_INTERNAL gboolean j_backend_data_read (JBackend*, JBackendItem*, gpointer, guint64, guint64, guint64*, gpointer);
J_GNUC_INTERNAL gboolean j_backend_data_write (JBackend*, JBackendItem*, gconstpointer, guint64, guint64, guint64*, gpointer);

J_GNUC_INTERNAL gboolean j_backend_meta_init (JBackend*, gchar const*);
J_GNUC_INTERNAL void j_backend_meta_fini (JBackend*);

J_GNUC_INTERNAL gpointer j_backend_meta_thread_init (JBackend*);
J_GNUC_INTERNAL void j_backend_meta_thread_fini (JBackend*, gpointer);

J_GNUC_INTERNAL gboolean j_backend_meta_batch_start (JBackend*, gchar const*, gpointer*);
J_GNUC_INTERNAL gboolean j_backend_meta_batch_execute (JBackend*, gpointer);

J_GNUC_INTERNAL gboolean j_backend_meta_put (JBackend*, gchar const*, bson_t const*, gpointer);
J_GNUC_INTERNAL gboolean j_backend_meta_delete (JBackend*, gchar const*, gpointer);
J_GNUC_INTERNAL gboolean j_backend_meta_get (JBackend*, gchar const*, gchar const*, bson_t*);

J_GNUC_INTERNAL gboolean j_backend_meta_get_all (JBackend*, gchar const*, gpointer*);
J_GNUC_INTERNAL gboolean j_backend_meta_get_by_value (JBackend*, gchar const*, bson_t const*, gpointer*);
J_GNUC_INTERNAL gboolean j_backend_meta_iterate (JBackend*, gpointer, bson_t*);

#endif
