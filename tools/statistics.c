/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#include <glib.h>

#include <julea.h>

#include <jconnection-internal.h>
#include <jmessage.h>

int
main (int argc, char** argv)
{
	JConfiguration* configuration;
	JMessage* message;
	gchar get_all;

	j_init(&argc, &argv);

	get_all = 1;
	configuration = j_configuration();

	message = j_message_new(J_MESSAGE_OPERATION_STATISTICS, sizeof(gchar));
	j_message_add_operation(message, 0);
	j_message_append_1(message, &get_all);

	for (guint i = 0; i < j_configuration_get_data_server_count(configuration); i++)
	{
		JMessage* reply;
		gchar* size;
		guint64 value;

		j_connection_send(j_connection(), i, message);

		reply = j_message_new_reply(message, 5 * sizeof(guint64));
		j_connection_receive(j_connection(), i, reply, message);

		g_print("Data server %d\n", i);

		value = j_message_get_8(reply);
		g_print("  %" G_GUINT64_FORMAT " files created\n", value);

		value = j_message_get_8(reply);
		g_print("  %" G_GUINT64_FORMAT " files deleted\n", value);

		value = j_message_get_8(reply);
		g_print("  %" G_GUINT64_FORMAT " syncs\n", value);

		value = j_message_get_8(reply);
		size = g_format_size(value);
		g_print("  %s read\n", size);
		g_free(size);

		value = j_message_get_8(reply);
		size = g_format_size(value);
		g_print("  %s written\n", size);
		g_free(size);

		if (i != j_configuration_get_data_server_count(configuration) - 1)
		{
			g_print("\n");
		}

		j_message_free(reply);
	}

	j_message_free(message);

	j_fini();

	return 0;
}
