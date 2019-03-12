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

#include <jlock.h>

#include "test.h"

static
void
test_lock_new_free (void)
{
	guint const n = 100;

	for (guint i = 0; i < n; i++)
	{
		JLock* lock;

		lock = j_lock_new("test", "path");
		g_assert(lock != NULL);
		j_lock_free(lock);
	}
}

static
void
test_lock_acquire_release (void)
{
	guint const n = 1000;

	for (guint i = 0; i < n; i++)
	{
		JLock* lock;
		gboolean ret;

		lock = j_lock_new("test", "path");

		ret = j_lock_acquire(lock);
		g_assert(ret);
		ret = j_lock_release(lock);
		g_assert(ret);

		j_lock_free(lock);
	}
}

static
void
test_lock_add (void)
{
	guint const n = 1000;

	JLock* lock;
	gboolean ret;

	lock = j_lock_new("test", "path");

	for (guint i = 0; i < n; i++)
	{
		j_lock_add(lock, i);
	}

	ret = j_lock_acquire(lock);
	g_assert(ret);
	ret = j_lock_release(lock);
	g_assert(ret);

	j_lock_free(lock);
}

void
test_lock (void)
{
	g_test_add_func("/lock/new_free", test_lock_new_free);
	g_test_add_func("/lock/acquire_release", test_lock_acquire_release);
	g_test_add_func("/lock/add", test_lock_add);
}
