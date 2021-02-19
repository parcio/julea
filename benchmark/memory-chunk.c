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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>

#include <jmemory-chunk.h>

#include "benchmark.h"

static void
benchmark_memory_chunk_get(BenchmarkRun* run)
{
	guint const n = 10000000;

	JMemoryChunk* memory_chunk;

	memory_chunk = j_memory_chunk_new(n);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			j_memory_chunk_get(memory_chunk, 1);
		}

		j_memory_chunk_reset(memory_chunk);
	}

	j_benchmark_timer_stop(run);

	j_memory_chunk_free(memory_chunk);

	run->operations = n;
}

void
benchmark_memory_chunk(void)
{
	j_benchmark_add("/memory-chunk/get-reset", benchmark_memory_chunk_get);
}
