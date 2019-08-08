/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#ifndef JULEA_SERVER_H
#define JULEA_SERVER_H

#include <glib.h>
#include <gio/gio.h>

#include <jbackend.h>
#include <jmemory-chunk.h>
#include <jmessage.h>
#include <jstatistics.h>

G_GNUC_INTERNAL JStatistics* jd_statistics;
G_GNUC_INTERNAL GMutex jd_statistics_mutex[1];

G_GNUC_INTERNAL JBackend* jd_object_backend;
G_GNUC_INTERNAL JBackend* jd_kv_backend;
G_GNUC_INTERNAL JBackend* jd_db_backend;

G_GNUC_INTERNAL gboolean jd_handle_message (JMessage*, GSocketConnection*, JMemoryChunk*, guint64, JStatistics*);

#endif
