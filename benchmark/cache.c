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

#include <jcache-internal.h>

#include "benchmark.h"

static
void
benchmark_cache_get (JCacheImplementation implementation, BenchmarkResult* result)
{
	guint const n = J_MIB(50);

	JCache* cache;
	gdouble elapsed;

	cache = j_cache_new(implementation, J_MIB(50));

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_cache_get(cache, 1);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_cache_free(cache);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_cache_chunk (BenchmarkResult* result)
{
	benchmark_cache_get(J_CACHE_CHUNK, result);
}

static
void
benchmark_cache_malloc (BenchmarkResult* result)
{
	benchmark_cache_get(J_CACHE_MALLOC, result);
}

void
benchmark_cache (void)
{
	j_benchmark_run("/cache/chunk", benchmark_cache_chunk);
	j_benchmark_run("/cache/malloc", benchmark_cache_malloc);
}
