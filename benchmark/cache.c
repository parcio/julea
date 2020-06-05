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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>

#include <jcache.h>

#include "benchmark.h"

static void
benchmark_cache_get_release(BenchmarkRun* run)
{
	guint const n = 100000;

	JCache* cache;

	cache = j_cache_new(n);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			gpointer buf;

			buf = j_cache_get(cache, 1);
			j_cache_release(cache, buf);
		}
	}

	j_benchmark_timer_stop(run);

	j_cache_free(cache);

	run->operations = n * 2;
}

void
benchmark_cache(void)
{
	j_benchmark_add("/cache/get-release", benchmark_cache_get_release);
}
