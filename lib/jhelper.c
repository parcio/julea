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

#include <julea-config.h>

#include <glib.h>
#include <gio/gio.h>

#include <bson.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <jhelper-internal.h>

#include <jsemantics.h>
#include <jtrace-internal.h>

/**
 * \defgroup JHelper Helper
 *
 * Helper data structures and functions.
 *
 * @{
 **/

void
j_helper_set_nodelay (GSocketConnection* connection, gboolean enable)
{
	gint const flag = (enable) ? 1 : 0;

	GSocket* socket_;
	gint fd;

	g_return_if_fail(connection != NULL);

	j_trace_enter(G_STRFUNC);

	socket_ = g_socket_connection_get_socket(connection);
	fd = g_socket_get_fd(socket_);

	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(gint));

	j_trace_leave(G_STRFUNC);
}

void
j_helper_set_cork (GSocketConnection* connection, gboolean enable)
{
	gint const flag = (enable) ? 1 : 0;

	GSocket* socket_;
	gint fd;

	g_return_if_fail(connection != NULL);

	j_trace_enter(G_STRFUNC);

	socket_ = g_socket_connection_get_socket(connection);
	fd = g_socket_get_fd(socket_);

	setsockopt(fd, IPPROTO_TCP, TCP_CORK, &flag, sizeof(gint));

	j_trace_leave(G_STRFUNC);
}

void
j_helper_get_number_string (gchar* string, guint32 length, guint32 number)
{
	gint ret;

	/* FIXME improve */
	ret = g_snprintf(string, length, "%d", number);
	g_return_if_fail((guint)ret <= length);
}

/**
 * @}
 **/
