/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2022 Michael Kuhn
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

#ifndef JULEA_HELPER_H
#define JULEA_HELPER_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>
#include <gio/gio.h>

#include <core/jbackground-operation.h>

G_BEGIN_DECLS

/**
 * Execudes a command which returns true on success.
 * on error a warning is written and jumps to <err_label> in the form:
 * EXE failed at <File>:<Line>: with
 * "<format string>", args
 *
 * \param cmd command to execute
 * \param err_label label to jump to, when failed
 * \param ... warning message in form: <format string>, args
 **/
#define EXE(cmd, err_label, ...) \
	do \
	{ \
		if ((cmd) == FALSE) \
		{ \
			g_warning("EXE failed at %s:%d with:", __FILE__, __LINE__); \
			g_warning(__VA_ARGS__); \
			goto err_label; \
		} \
	} while (FALSE)

/**
 * Checks if <res> is an non negative number, if it negative jumps to <err_label>
 * and print a warning in the form:
 * CHECK failed at <File>:<Line> with (<res>):
 * "<format string>", args
 * 
 * \param res result value
 * \param err_label label to jump to at error
 * \param ... warning message in form: <format string>, args
 **/
#define CHECK(res, err_label, ...) \
	do \
	{ \
		if (res < 0) \
		{ \
			g_warning("CHECK failed at %s:%d with (%d):", __FILE__, __LINE__, res); \
			g_warning(__VA_ARGS__); \
			goto err_label; \
		} \
	} while (FALSE)

/**
 * Check if <error> is an filled GError, if print warning and jumps to <err_label>
 * warnings has the form:
 * CHECK failed at <File>:<Line> with:
 * "<format string>", args
 *
 * \param error GError
 * \param err_label label to jump to at error
 * \param ... warning message in form: <format sting>, args
 **/
#define CHECK_GERROR(error, err_label, ...) \
	do \
	{ \
		if (error != NULL) \
		{ \
			g_warning("CHECK failed at %s:%d with:\n\t%s", __FILE__, __LINE__, error->message); \
			g_error_free(error); \
			goto err_label; \
		} \
	} while (FALSE)

/**
 * \defgroup JHelper Helper
 *
 * Helper data structures and functions.
 *
 * @{
 **/

/**
 * Convert a number to a string.
 *
 * \param string The buffer to be filled with the number string.
 * \param length The size of the string buffer.
 * \param number The number to convert.
 *
 * \remark If the buffer is to small not the whole number will be written.
 **/
void j_helper_get_number_string(gchar* string, guint32 length, guint32 number);

/**
 * Set the TCP_CORK flag to the value of enable
 *
 * \param connection The connection to manipulate.
 * \param enable     The value to be set for TCP_CORK.
 *
 * \todo get rid of GSocketConnection
 *
 **/
void j_helper_set_cork(GSocketConnection* connection, gboolean enable);

/**
 * Set the TCP_NODELAY flag to the value of enable
 *
 * \param connection The connection to manipulate.
 * \param enable     The value to be set for TCP_NODELAY.
 *
 * \todo get rid of GSocketConnection
 *
 **/
void j_helper_set_nodelay(GSocketConnection* connection, gboolean enable);

/**
 * Atomically add \p val to \p *ptr and return the result.
 *
 * \param ptr Address of a 64 bit unsigned integer.
 * \param val Value to add to \p *ptr.
 *
 * \remark If no atomic operation is available locking will bew used.
 **/
guint64 j_helper_atomic_add(guint64 volatile* ptr, guint64 val);

/**
 * Execute \p func in parallel.
 *
 * \param func   A JBackgroundOperationFunc.
 * \param data   An argument array for parallel execution of \p func.
 * \param length The length of \p data.
 *
 * \remark NULL is allowed to appear in \p data but \p func will not be executed with NULL as argument.
 **/
gboolean j_helper_execute_parallel(JBackgroundOperationFunc func, gpointer* data, guint length);

/**
 * A hash function for strings
 *
 * \param str The string to be hashed.
 **/
guint32 j_helper_hash(gchar const* str);

/**
 * Replaces all occurences of \p old with \p new in \p str in a new string.
 *
 * \param str A string.
 * \param old A string.
 * \param new A string.
 *
 * \return A new string containing the replacements. Should be freed with g_free().
 **/
gchar* j_helper_str_replace(gchar const* str, gchar const* old, gchar const* new);

/**
 * Error checking wrapper for aligned_alloc().
 *
 * \param align The alignment to use. Must be support by implementation.
 * \param len   The number of bytes to allocate.
 *
 * \return A pointer to allocated memory or NULL on failure. Memory should be freed with free().
 **/
gpointer j_helper_alloc_aligned(gsize align, gsize len);

/**
 * Synchronizes the file described by \p path with the storage device.
 *
 * \param path The file to sync.
 *
 * \return TRUE on success FALSE otherwise.
 **/
gboolean j_helper_file_sync(gchar const* path);

/**
 * Synchronizes the file described by \p path with the storage device.
 * The file will be marked as not accessed in the near future to make optimization in kernel possible.
 *
 * \param path The file to sync.
 *
 * \remark This only states an intention and is not binding.
 *
 * \return TRUE on success FALSE otherwise.
 **/
gboolean j_helper_file_discard(gchar const* path);

/**
 * @}
 **/

G_END_DECLS

#endif
