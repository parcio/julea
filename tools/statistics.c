/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>

#include <jcommon-internal.h>
#include <jconnection-internal.h>
#include <jconnection-pool-internal.h>
#include <jmessage.h>
#include <jstatistics-internal.h>

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
	JConnection* connection;
	JMessage* message;
	JStatistics* statistics_total;
	gchar get_all;

	j_init();

	get_all = 1;
	configuration = j_configuration();
	connection = j_connection_pool_pop();
	statistics_total = j_statistics_new(FALSE);

	message = j_message_new(J_MESSAGE_STATISTICS, sizeof(gchar));
	j_message_add_operation(message, 0);
	j_message_append_1(message, &get_all);

	for (guint i = 0; i < j_configuration_get_data_server_count(configuration); i++)
	{
		JMessage* reply;
		JStatistics* statistics;
		guint64 value;

		statistics = j_statistics_new(FALSE);

		j_connection_send(connection, i, message);

		reply = j_message_new_reply(message);
		j_connection_receive(connection, i, reply);

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

		j_message_unref(reply);

		g_print("Data server %d\n", i);
		print_statistics(statistics);

		if (i != j_configuration_get_data_server_count(configuration) - 1)
		{
			g_print("\n");
		}

		j_statistics_free(statistics);
	}

	if (j_configuration_get_data_server_count(configuration) > 1)
	{
		g_print("\n");
		g_print("Total\n");
		print_statistics(statistics_total);
	}

	j_message_unref(message);
	j_connection_pool_push(connection);
	j_statistics_free(statistics_total);

	j_fini();

	return 0;
}
