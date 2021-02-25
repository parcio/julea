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

#ifndef JULEA_BENCHMARK_H
#define JULEA_BENCHMARK_H

#include <glib.h>

struct BenchmarkRun
{
	gchar* name;
	void (*func)(struct BenchmarkRun*);
	GTimer* timer;
	gboolean timer_started;
	guint iterations;
	guint64 operations;
	guint64 bytes;

	gchar* namespace;
};

typedef struct BenchmarkRun BenchmarkRun;

#include <jsemantics.h>

typedef void (*BenchmarkFunc)(BenchmarkRun*);

JSemantics* j_benchmark_get_semantics(void);

void j_benchmark_timer_start(BenchmarkRun*);
void j_benchmark_timer_stop(BenchmarkRun*);

gboolean j_benchmark_iterate(BenchmarkRun*);

void j_benchmark_add(gchar const*, BenchmarkFunc);

void benchmark_background_operation(void);
void benchmark_cache(void);
void benchmark_memory_chunk(void);
void benchmark_message(void);

void benchmark_kv(void);

void benchmark_distributed_object(void);
void benchmark_object(void);

void benchmark_db_entry(void);
void benchmark_db_iterator(void);
void benchmark_db_schema(void);

void benchmark_collection(void);
void benchmark_item(void);

void benchmark_hdf(void);
void benchmark_hdf_dai(void);

#endif
