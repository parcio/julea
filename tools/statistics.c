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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>

#include <jcommon.h>
#include <jconnection-pool.h>
#include <jmessage.h>
#include <jstatistics.h>

static
void
print_statistics (JStatistics* statistics)
{
	gchar* size_read;
	gchar* size_written;
	gchar* size_received;
	gchar* size_sent;

	size_read = g_format_size(j_statistics_get(statistics, J_STATISTICS_BYTES_READ));
	size_written = g_format_size(j_statistics_get(statistics, J_STATISTICS_BYTES_WRITTEN));
	size_received = g_format_size(j_statistics_get(statistics, J_STATISTICS_BYTES_RECEIVED));
	size_sent = g_format_size(j_statistics_get(statistics, J_STATISTICS_BYTES_SENT));

	g_print("  %" G_GUINT64_FORMAT " files created\n", j_statistics_get(statistics, J_STATISTICS_FILES_CREATED));
	g_print("  %" G_GUINT64_FORMAT " files deleted\n", j_statistics_get(statistics, J_STATISTICS_FILES_DELETED));
	g_print("  %" G_GUINT64_FORMAT " files stat'ed\n", j_statistics_get(statistics, J_STATISTICS_FILES_STATED));
	g_print("  %" G_GUINT64_FORMAT " syncs\n", j_statistics_get(statistics, J_STATISTICS_SYNC));
	g_print("  %s read\n", size_read);
	g_print("  %s written\n", size_written);
	g_print("  %s received\n", size_received);
	g_print("  %s sent\n", size_sent);

	g_free(size_read);
	g_free(size_written);
	g_free(size_received);
	g_free(size_sent);
}

int
main (int argc, char** argv)
{
	JConfiguration* configuration;
	g_autoptr(JMessage) message = NULL;
	JStatistics* statistics_total;
	gchar get_all;

	(void)argc;
	(void)argv;

	get_all = 1;
	configuration = j_configuration();
	statistics_total = j_statistics_new(FALSE);

	message = j_message_new(J_MESSAGE_STATISTICS, sizeof(gchar));
	j_message_add_operation(message, 0);
	j_message_append_1(message, &get_all);

	for (guint i = 0; i < j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT); i++)
	{
		g_autoptr(JMessage) reply = NULL;
		JStatistics* statistics;
		gpointer connection;
		guint64 value;

		connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, i);
		statistics = j_statistics_new(FALSE);

		j_message_send(message, connection);

		reply = j_message_new_reply(message);
		j_message_receive(reply, connection);

		value = j_message_get_8(reply);
		j_statistics_add(statistics, J_STATISTICS_FILES_CREATED, value);
		j_statistics_add(statistics_total, J_STATISTICS_FILES_CREATED, value);

		value = j_message_get_8(reply);
		j_statistics_add(statistics, J_STATISTICS_FILES_DELETED, value);
		j_statistics_add(statistics_total, J_STATISTICS_FILES_DELETED, value);

		value = j_message_get_8(reply);
		j_statistics_add(statistics, J_STATISTICS_FILES_STATED, value);
		j_statistics_add(statistics_total, J_STATISTICS_FILES_STATED, value);

		value = j_message_get_8(reply);
		j_statistics_add(statistics, J_STATISTICS_SYNC, value);
		j_statistics_add(statistics_total, J_STATISTICS_SYNC, value);

		value = j_message_get_8(reply);
		j_statistics_add(statistics, J_STATISTICS_BYTES_READ, value);
		j_statistics_add(statistics_total, J_STATISTICS_BYTES_READ, value);

		value = j_message_get_8(reply);
		j_statistics_add(statistics, J_STATISTICS_BYTES_WRITTEN, value);
		j_statistics_add(statistics_total, J_STATISTICS_BYTES_WRITTEN, value);

		value = j_message_get_8(reply);
		j_statistics_add(statistics, J_STATISTICS_BYTES_RECEIVED, value);
		j_statistics_add(statistics_total, J_STATISTICS_BYTES_RECEIVED, value);

		value = j_message_get_8(reply);
		j_statistics_add(statistics, J_STATISTICS_BYTES_SENT, value);
		j_statistics_add(statistics_total, J_STATISTICS_BYTES_SENT, value);

		g_print("Data server %d\n", i);
		print_statistics(statistics);

		if (i != j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT) - 1)
		{
			g_print("\n");
		}

		j_statistics_free(statistics);
		j_connection_pool_push(J_BACKEND_TYPE_OBJECT, i, connection);
	}

	if (j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT) > 1)
	{
		g_print("\n");
		g_print("Total\n");
		print_statistics(statistics_total);
	}

	j_statistics_free(statistics_total);

	return 0;
}
