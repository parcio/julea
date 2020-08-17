/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2020 Michael Kuhn
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

#ifndef JULEA_OBJECT_OBJECT_INTERNAL_H
#define JULEA_OBJECT_OBJECT_INTERNAL_H

#if !defined(JULEA_OBJECT_H) && !defined(JULEA_OBJECT_COMPILATION)
#error "Only <julea-object.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

#include <core/jmessage.h>

G_BEGIN_DECLS

G_GNUC_INTERNAL JBackend* j_object_get_backend(void);

// internal data chunk receive for jobject
G_GNUC_INTERNAL gboolean j_object_receive_data_chunks(JMessage*, JEndpoint*, JListIterator*, guint32*);
G_GNUC_INTERNAL gboolean j_object_receive_data_chunks_msg(JMessage*, JEndpoint*, JListIterator*, guint32*);
G_GNUC_INTERNAL gboolean j_object_receive_data_chunks_rdma(JMessage*, JEndpoint*, JListIterator*, guint32*);

// internal data chunk receive for jdistributed_object
G_GNUC_INTERNAL gboolean j_distributed_object_receive_data_chunks(JMessage*, JEndpoint*, JListIterator*, guint32*);
G_GNUC_INTERNAL gboolean j_distributed_object_receive_data_chunks_msg(JMessage*, JEndpoint*, JListIterator*, guint32*);
G_GNUC_INTERNAL gboolean j_distributed_object_receive_data_chunks_rdma(JMessage*, JEndpoint*, JListIterator*, guint32*);

G_END_DECLS

#endif
