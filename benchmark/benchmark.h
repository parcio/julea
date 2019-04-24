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

#ifndef JULEA_BENCHMARK_H
#define JULEA_BENCHMARK_H

#include <glib.h>

struct BenchmarkResult
{
	gdouble elapsed_time;
	guint64 operations;
	guint64 bytes;
};

typedef struct BenchmarkResult BenchmarkResult;

#include <jsemantics.h>

typedef void (*BenchmarkFunc) (BenchmarkResult*);

JSemantics* j_benchmark_get_semantics (void);

void j_benchmark_timer_start (void);
gdouble j_benchmark_timer_elapsed (void);

void j_benchmark_run (gchar const*, BenchmarkFunc);

void benchmark_background_operation (void);
void benchmark_cache (void);
void benchmark_lock (void);
void benchmark_memory_chunk (void);
void benchmark_message (void);

void benchmark_kv (void);

void benchmark_distributed_object (void);
void benchmark_object (void);

void benchmark_collection (void);
void benchmark_item (void);

void benchmark_hdf (void);

#endif
