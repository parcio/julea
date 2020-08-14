/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#ifndef JULEA_THREAD_H
#define JULEA_THREAD_H

#include <glib.h>
#include <gio/gio.h>

#include <jbackend.h>
#include <jmemory-chunk.h>
#include <jmessage.h>
#include <jstatistics.h>

#include <netinet/in.h> //for target address
#include <sys/socket.h>
#include <arpa/inet.h>

#include <rdma/fi_domain.h>

struct JFabric
{
	struct fid_fabric* msg_fabric;
	struct fi_info* msg_info;
	struct fid_fabric* rdma_fabric;
	struct fi_info* rdma_info;
};
typedef struct JFabric JFabric;

struct ThreadData
{
	struct
	{
		struct fi_eq_cm_entry* msg_event;
		struct fi_eq_cm_entry* rdma_event;
		JFabric* fabric;
		JConfiguration* j_configuration;
		JDomainManager* domain_manager;
		gchar* uuid;
	} connection;

	struct
	{
		GPtrArray* thread_cq_array; // for registration in waking threads
		GMutex* thread_cq_array_mutex; // for registration in waking threads
		volatile gboolean* server_running;
		volatile gboolean* thread_running;
		volatile gint* thread_count;
		JStatistics* j_statistics;
		GMutex* j_statistics_mutex;
	} julea_state;
};
typedef struct ThreadData ThreadData;

G_GNUC_INTERNAL JStatistics* j_statistics;
G_GNUC_INTERNAL GMutex* j_statistics_mutex;

gpointer j_thread_function(gpointer connection_event_entry);
G_GNUC_INTERNAL gboolean j_thread_libfabric_ress_init(gpointer, JEndpoint**);
G_GNUC_INTERNAL void j_thread_libfabric_ress_shutdown(JEndpoint*);
void thread_unblock(struct fid_cq* completion_queue);

G_GNUC_INTERNAL gboolean jd_handle_message(JMessage*, JEndpoint*, JMemoryChunk*, guint64, JStatistics*);
G_GNUC_INTERNAL void handle_chunks_msg(JMessage*,	JEndpoint*, guint32, JMemoryChunk*, guint64, gpointer, JStatistics*, JMessage*);
G_GNUC_INTERNAL void handle_chunks_rdma(JMessage*,	JEndpoint*, guint32, JMemoryChunk*, guint64, gpointer, JStatistics*, JMessage*);

#endif
