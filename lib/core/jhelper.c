/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2021 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>
#include <glib/gstdio.h>
#include <gio/gio.h>

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <jhelper.h>

#include <jbackground-operation.h>
#include <jsemantics.h>
#include <jtrace.h>

/**
 * \addtogroup JHelper Helper
 *
 * Helper data structures and functions.
 *
 * @{
 **/

void
j_helper_set_nodelay(GSocketConnection* connection, gboolean enable)
{
	J_TRACE_FUNCTION(NULL);

	gint const flag = (enable) ? 1 : 0;

	GSocket* socket_;
	gint fd;

	g_return_if_fail(connection != NULL);

	socket_ = g_socket_connection_get_socket(connection);
	fd = g_socket_get_fd(socket_);

	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(gint));
}

void
j_helper_set_cork(GSocketConnection* connection, gboolean enable)
{
	J_TRACE_FUNCTION(NULL);

	gint const flag = (enable) ? 1 : 0;

	GSocket* socket_;
	gint fd;

	g_return_if_fail(connection != NULL);

	socket_ = g_socket_connection_get_socket(connection);
	fd = g_socket_get_fd(socket_);

	setsockopt(fd, IPPROTO_TCP, TCP_CORK, &flag, sizeof(gint));
}

void
j_helper_get_number_string(gchar* string, guint32 length, guint32 number)
{
	J_TRACE_FUNCTION(NULL);

	gint ret;

	/// \todo improve
	ret = g_snprintf(string, length, "%d", number);
	g_return_if_fail((guint)ret <= length);
}

gchar*
j_helper_str_replace(gchar const* str, gchar const* old, gchar const* new)
{
	J_TRACE_FUNCTION(NULL);

	g_autoptr(GRegex) regex = NULL;
	g_autofree gchar* old_escaped = NULL;
	gchar* replace;

	g_return_val_if_fail(str != NULL, NULL);
	g_return_val_if_fail(old != NULL, NULL);
	g_return_val_if_fail(new != NULL, NULL);

	old_escaped = g_regex_escape_string(old, -1);
	regex = g_regex_new(old_escaped, 0, 0, NULL);
	replace = g_regex_replace_literal(regex, str, -1, 0, new, 0, NULL);

	return replace;
}

gboolean
j_helper_execute_parallel(JBackgroundOperationFunc func, gpointer* data, guint length)
{
	J_TRACE_FUNCTION(NULL);

	JBackgroundOperation** operations;
	guint data_count = 0;

	operations = g_new(JBackgroundOperation*, length);

	for (guint i = 0; i < length; i++)
	{
		operations[i] = NULL;

		if (data[i] != NULL)
		{
			data_count++;
		}
	}

	for (guint i = 0; i < length; i++)
	{
		if (data[i] == NULL)
		{
			continue;
		}

		if (data_count > 1)
		{
			operations[i] = j_background_operation_new(func, data[i]);
		}
		else
		{
			data[i] = func(data[i]);
		}
	}

	for (guint i = 0; i < length; i++)
	{
		if (operations[i] != NULL)
		{
			data[i] = j_background_operation_wait(operations[i]);
			j_background_operation_unref(operations[i]);
		}
	}

	g_free(operations);

	return TRUE;
}

guint64
j_helper_atomic_add(guint64 volatile* ptr, guint64 val)
{
	J_TRACE_FUNCTION(NULL);

	guint64 ret;

	/// \todo check C11 atomic_fetch_add
#ifdef HAVE_SYNC_FETCH_AND_ADD
	ret = __sync_fetch_and_add(ptr, val);
#else
	G_LOCK_DEFINE_STATIC(j_helper_atomic_add);

	G_LOCK(j_helper_atomic_add);
	ret = *ptr;
	*ptr += val;
	G_UNLOCK(j_helper_atomic_add);
#endif

	return ret;
}

guint32
j_helper_hash(gchar const* str)
{
	J_TRACE_FUNCTION(NULL);

	gchar c;
	guint32 hash;

	hash = 5381;

	while ((c = *str++) != '\0')
	{
		hash = ((hash << 5) + hash) + c;
	}

	return hash;
}

gpointer
j_helper_alloc_aligned(gsize align, gsize len)
{
	gpointer buf;

	buf = aligned_alloc(align, len);

	if (buf == NULL)
	{
		g_error("Failed to allocate %" G_GSIZE_FORMAT " bytes with %" G_GSIZE_FORMAT " byte alignment", len, align);
	}

	return buf;
}

gboolean
j_helper_file_sync(gchar const* path)
{
	gint fd;

	if ((fd = g_open(path, O_RDWR)) == -1)
	{
		return FALSE;
	}

#if GLIB_CHECK_VERSION(2, 64, 0)
	if (g_fsync(fd) == -1)
#else
	if (fsync(fd) == -1)
#endif
	{
		return FALSE;
	}

	return g_close(fd, NULL);
}

gboolean
j_helper_file_discard(gchar const* path)
{
	gint fd;

	if ((fd = g_open(path, O_RDWR)) == -1)
	{
		return FALSE;
	}

#if GLIB_CHECK_VERSION(2, 64, 0)
	if (g_fsync(fd) == -1)
#else
	if (fsync(fd) == -1)
#endif
	{
		return FALSE;
	}

	if (posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) != 0)
	{
		return FALSE;
	}

	return g_close(fd, NULL);
}

/**
 * @}
 **/
