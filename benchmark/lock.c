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

#include <string.h>

#include <julea.h>

#include <jlock.h>

#include "benchmark.h"

static
void
_benchmark_lock (BenchmarkResult* result, gboolean acquire, gboolean add)
{
	guint const n = 3000;

	g_autoptr(JList) list = NULL;
	g_autoptr(JListIterator) iterator = NULL;
	JLock* lock;
	gdouble elapsed;

	list = j_list_new((JListFreeFunc)j_lock_free);

	if (acquire)
	{
		j_benchmark_timer_start();
	}

	for (guint i = 0; i < n; i++)
	{
		lock = j_lock_new("benchmark", "path");

		if (add)
		{
			for (guint j = 0; j < 10; j++)
			{
				/* Avoid overlap. */
				j_lock_add(lock, n + (i * 10) + j);
			}
		}

		j_lock_add(lock, i);
		j_lock_acquire(lock);
		j_list_append(list, lock);
	}

	if (acquire)
	{
		elapsed = j_benchmark_timer_elapsed();
	}

	iterator = j_list_iterator_new(list);

	if (!acquire)
	{
		j_benchmark_timer_start();
	}

	while (j_list_iterator_next(iterator))
	{
		lock = j_list_iterator_get(iterator);

		j_lock_release(lock);
	}

	if (!acquire)
	{
		elapsed = j_benchmark_timer_elapsed();
	}

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_lock_acquire (BenchmarkResult* result)
{
	_benchmark_lock(result, TRUE, FALSE);
}

static
void
benchmark_lock_release (BenchmarkResult* result)
{
	_benchmark_lock(result, FALSE, FALSE);
}

static
void
benchmark_lock_add (BenchmarkResult* result)
{
	_benchmark_lock(result, TRUE, TRUE);
}

void
benchmark_lock (void)
{
	j_benchmark_run("/lock/acquire", benchmark_lock_acquire);
	j_benchmark_run("/lock/release", benchmark_lock_release);
	j_benchmark_run("/lock/add", benchmark_lock_add);
}
