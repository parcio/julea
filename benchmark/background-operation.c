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

#include <jbackground-operation.h>

#include "benchmark.h"

gint benchmark_background_operation_counter;

static
gpointer
on_background_operation_completed (gpointer data)
{
	(void)data;

	g_atomic_int_inc(&benchmark_background_operation_counter);

	return NULL;
}

static
void
benchmark_background_operation_new_ref_unref (BenchmarkResult* result)
{
	guint const n = 100000;

	JBackgroundOperation* background_operation;
	gdouble elapsed;

	g_atomic_int_set(&benchmark_background_operation_counter, 0);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		background_operation = j_background_operation_new(on_background_operation_completed, NULL);
		j_background_operation_unref(background_operation);
	}

	/* FIXME overhead? */
	while (g_atomic_int_get(&benchmark_background_operation_counter) != (gint)n)
	{
		;
	}

	elapsed = j_benchmark_timer_elapsed();

	result->elapsed_time = elapsed;
	result->operations = n;
}

void
benchmark_background_operation (void)
{
	j_benchmark_run("/background-operation", benchmark_background_operation_new_ref_unref);
}
