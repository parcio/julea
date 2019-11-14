/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
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

#ifndef BENCHMARK_MPI_C
//compile for code formatting reasons
#include "benchmark-mpi.c"
#endif

#ifndef BENCHMARK_MPI_MAIN_C
#define BENCHMARK_MPI_MAIN_C

#include "benchmark-mpi.h"

// options -->>
static
const guint batch_size = 10000;
static
const gdouble allowed_percentage = 0.8;
static
gdouble target_time_low = 30.0;
static
gdouble target_time_high = 30.0;
static
guint multiplicator = 2;
// <<-- options

static
char namespace[100];

struct BenchmarkResult
{
	guint64 operations;
	guint64 operations_without_n;
	gdouble elapsed_time;
	gdouble prognosted_time;
};
typedef struct BenchmarkResult BenchmarkResult;

static
GTimer* j_benchmark_timer = NULL;

static
void
j_benchmark_timer_start(void)
{
	MPI_Barrier(MPI_COMM_WORLD);
	//start simultaneously
	g_timer_start(j_benchmark_timer);
}

static
gdouble
j_benchmark_timer_elapsed(void)
{
	gdouble elapsed;
	gdouble elapsed_global;
	MPI_Barrier(MPI_COMM_WORLD);
	//wait for the slowest
	elapsed = g_timer_elapsed(j_benchmark_timer, NULL);
	MPI_Allreduce(&elapsed, &elapsed_global, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
	MPI_Barrier(MPI_COMM_WORLD);
	return elapsed_global / (gdouble)world_size;
}

struct result_step
{
	guint n;
	BenchmarkResult schema_create[2];
	BenchmarkResult schema_get[2];
	BenchmarkResult schema_delete[2];
	//
	BenchmarkResult schema_new;
	BenchmarkResult schema_free;
	//
	BenchmarkResult schema_ref;
	BenchmarkResult schema_unref;
	//
	BenchmarkResult schema_equals;
	BenchmarkResult schema_add_field;
	BenchmarkResult schema_get_field;
	BenchmarkResult schema_get_fields;
	//
	BenchmarkResult entry_new;
	BenchmarkResult entry_free;
	BenchmarkResult entry_set_field;
	//
	BenchmarkResult entry_ref;
	BenchmarkResult entry_unref;
	//
	BenchmarkResult entry_insert[12];
	BenchmarkResult entry_update[12];
	BenchmarkResult entry_delete[12];
	BenchmarkResult iterator_single[12];
	BenchmarkResult iterator_all[12];
};
typedef struct result_step result_step;
static
result_step* current_result_step = NULL;
static
result_step* next_result_step = NULL;
static
result_step* all_result_step = NULL;

#include "benchmark-mpi-entry.c"
#include "benchmark-mpi-schema.c"
static
void
myprintf(const char* name, guint n, BenchmarkResult* result)
{
	if (world_rank == 0 &&
		result->elapsed_time > 0 && result->operations > 0)
	{
		printf("/db/%d/%s %.3f seconds (%.0f / s) [ %ld %ld ]\n",
			n,
			name,
			result->elapsed_time,
			(gdouble)result->operations / result->elapsed_time,
			result->operations,
			result->operations_without_n);
		fflush(stdout);
	}
}
static
void
myprintf2(const char* name, guint n, guint n2, BenchmarkResult* result)
{
	if (world_rank == 0 && result->elapsed_time > 0 && result->operations > 0)
	{
		printf("/db/%d/%d/%s %.3f seconds (%.0f / s) [ %ld %ld ]\n",
			n,
			n2,
			name,
			result->elapsed_time,
			(gdouble)result->operations / result->elapsed_time,
			result->operations,
			result->operations_without_n);
		fflush(stdout);
	}
}

static
void
exec_tests(guint n)
{
	guint my_index;
	guint n2;
	guint n1 = ((n / world_size) + ((n / world_size) == 0)) * world_size;
	MPI_Barrier(MPI_COMM_WORLD);
	if (n <= 512 && world_size == 1)
	{
		{
			_benchmark_db_schema_create(TRUE, n);
			_benchmark_db_schema_create(FALSE, n);
			myprintf("schema/create", n, &current_result_step->schema_create[FALSE]);
			myprintf("schema/get", n, &current_result_step->schema_get[FALSE]);
			myprintf("schema/delete", n, &current_result_step->schema_delete[FALSE]);
			myprintf("schema/create-batch", n, &current_result_step->schema_create[TRUE]);
			myprintf("schema/get-batch", n, &current_result_step->schema_get[TRUE]);
			myprintf("schema/delete-batch", n, &current_result_step->schema_delete[TRUE]);
		}
		{
			_benchmark_db_schema_ref();
			myprintf("schema/ref", n, &current_result_step->schema_ref);
			myprintf("schema/unref", n, &current_result_step->schema_unref);
		}
		{
			_benchmark_db_schema_new();
			myprintf("schema/new", n, &current_result_step->schema_new);
			myprintf("schema/free", n, &current_result_step->schema_free);
		}
		{
			_benchmark_db_schema_add_field(n);
			myprintf("schema/add_field", n, &current_result_step->schema_add_field);
			myprintf("schema/get_field", n, &current_result_step->schema_get_field);
			myprintf("schema/get_fields", n, &current_result_step->schema_get_fields);
			myprintf("schema/equals", n, &current_result_step->schema_equals);
		}
		{
			_benchmark_db_entry_ref();
			myprintf("entry/ref", n, &current_result_step->entry_ref);
			myprintf("entry/unref", n, &current_result_step->entry_unref);
		}
		{
			_benchmark_db_entry_new();
			myprintf("entry/new", n, &current_result_step->entry_new);
			myprintf("entry/free", n, &current_result_step->entry_free);
		}
		{
			_benchmark_db_entry_set_field(n);
			myprintf("entry/set_field", n, &current_result_step->entry_set_field);
		}
	}
	my_index = 0;
	for (n2 = 5; n2 <= 500; n2 *= 10, my_index += 4)
	{
		{
			_benchmark_db_entry_insert(FALSE, FALSE, n1, n2, J_SEMANTICS_ATOMICITY_NONE);
			myprintf2("entry/insert", n1, n2, &current_result_step->entry_insert[my_index + 0]);
			myprintf2("entry/update", n1, n2, &current_result_step->entry_update[my_index + 0]);
			myprintf2("entry/delete", n1, n2, &current_result_step->entry_delete[my_index + 0]);
		}
		{
			_benchmark_db_entry_insert(TRUE, FALSE, n1, n2, J_SEMANTICS_ATOMICITY_NONE);
			myprintf2("entry/insert-batch", n1, n2, &current_result_step->entry_insert[my_index + 1]);
			myprintf2("entry/update-batch", n1, n2, &current_result_step->entry_update[my_index + 1]);
			myprintf2("entry/delete-batch", n1, n2, &current_result_step->entry_delete[my_index + 1]);
			myprintf2("iterator/single", n1, n2, &current_result_step->iterator_single[my_index + 1]);
			myprintf2("iterator/all", n1, n2, &current_result_step->iterator_all[my_index + 1]);
		}
		{
			_benchmark_db_entry_insert(TRUE, TRUE, n1, n2, J_SEMANTICS_ATOMICITY_NONE);
			myprintf2("entry/insert-batch-index", n1, n2, &current_result_step->entry_insert[my_index + 2]);
			myprintf2("entry/update-batch-index", n1, n2, &current_result_step->entry_update[my_index + 2]);
			myprintf2("entry/delete-batch-index", n1, n2, &current_result_step->entry_delete[my_index + 2]);
			myprintf2("iterator/single-index", n1, n2, &current_result_step->iterator_single[my_index + 2]);
			myprintf2("iterator/all-index", n1, n2, &current_result_step->iterator_all[my_index + 2]);
		}
		{
			_benchmark_db_entry_insert(TRUE, TRUE, n1, n2, J_SEMANTICS_ATOMICITY_BATCH);
			myprintf2("entry/insert-batch-index-atomicity", n1, n2, &current_result_step->entry_insert[my_index + 3]);
			myprintf2("entry/update-batch-index-atomicity", n1, n2, &current_result_step->entry_update[my_index + 3]);
			myprintf2("entry/delete-batch-index-atomicity", n1, n2, &current_result_step->entry_delete[my_index + 3]);
			myprintf2("iterator/single-index-atomicity", n1, n2, &current_result_step->iterator_single[my_index + 3]);
			myprintf2("iterator/all-index-atomicity", n1, n2, &current_result_step->iterator_all[my_index + 3]);
		}
	}
}

#define prognose_2(p_next, p_curr)                                                                                                     \
	do                                                                                                                             \
	{                                                                                                                              \
		if (p_curr.operations_without_n == 0 || (p_curr.operations_without_n <= 1 && p_curr.elapsed_time >= target_time_high)) \
			p_next.prognosted_time = target_time_high + 1;                                                                 \
		else                                                                                                                   \
			p_next.prognosted_time = 0;                                                                                    \
		p_next.elapsed_time = 0;                                                                                               \
		p_next.operations_without_n = 0;                                                                                       \
		p_next.operations = 0;                                                                                                 \
		result = result || (p_next.prognosted_time < target_time_high && p_curr.operations_without_n > 0);                     \
	} while (0)

static
gboolean
calculate_prognose(gint n_next)
{
	gboolean result = FALSE;
	guint j;
	guint my_index;

	memset(next_result_step, 0, sizeof(*next_result_step));

	next_result_step->n = n_next;
	{
		prognose_2(next_result_step->schema_equals, current_result_step->schema_equals);
		prognose_2(next_result_step->schema_add_field, current_result_step->schema_add_field);
		prognose_2(next_result_step->schema_get_field, current_result_step->schema_get_field);
		prognose_2(next_result_step->schema_get_fields, current_result_step->schema_get_fields);
		prognose_2(next_result_step->entry_set_field, current_result_step->entry_set_field);
		for (j = 0; j < 2; j++)
		{
			prognose_2(next_result_step->schema_create[j], current_result_step->schema_create[j]);
			prognose_2(next_result_step->schema_get[j], current_result_step->schema_get[j]);
			prognose_2(next_result_step->schema_delete[j], current_result_step->schema_delete[j]);
		}
		for (j = 0; j < 12; j++)
		{
			prognose_2(next_result_step->entry_insert[j], current_result_step->entry_insert[j]);
			prognose_2(next_result_step->entry_update[j], current_result_step->entry_update[j]);
			prognose_2(next_result_step->entry_delete[j], current_result_step->entry_delete[j]);
			prognose_2(next_result_step->iterator_single[j], current_result_step->iterator_single[j]);
			prognose_2(next_result_step->iterator_all[j], current_result_step->iterator_all[j]);
		}
	}
	next_result_step->schema_new.prognosted_time = target_time_high + 1;
	next_result_step->schema_free.prognosted_time = target_time_high + 1;
	next_result_step->schema_ref.prognosted_time = target_time_high + 1;
	next_result_step->schema_unref.prognosted_time = target_time_high + 1;
	next_result_step->entry_new.prognosted_time = target_time_high + 1;
	next_result_step->entry_free.prognosted_time = target_time_high + 1;
	next_result_step->entry_ref.prognosted_time = target_time_high + 1;
	next_result_step->entry_unref.prognosted_time = target_time_high + 1;
	for (my_index = 0; my_index < 12; my_index += 4)
	{
		next_result_step->iterator_single[my_index].prognosted_time = target_time_high + 1;
		next_result_step->iterator_all[my_index].prognosted_time = target_time_high + 1;
	}
	return result;
}

static
void
benchmark_db(void)
{
	const char* target_low_str;
	const char* target_high_str;
	int ret;
	double target_low = 0.0;
	double target_high = 0.0;
	result_step* tmp;
	guint n;
	guint n_next;
	sprintf(namespace, "namespace_%d", world_rank);
	MPI_Barrier(MPI_COMM_WORLD);
	target_low_str = g_getenv("J_BENCHMARK_TARGET_LOW");
	if (target_low_str)
	{
		g_debug("J_BENCHMARK_TARGET_LOW %s", target_low_str);
		ret = sscanf(target_low_str, "%lf", &target_low);
		if (ret == 1)
		{
			g_debug("J_BENCHMARK_TARGET_LOW %s %f", target_low_str, target_low);
			target_time_low = target_low;
		}
	}
	target_high_str = g_getenv("J_BENCHMARK_TARGET_HIGH");
	if (target_high_str)
	{
		g_debug("J_BENCHMARK_TARGET_HIGH %s", target_high_str);
		ret = sscanf(target_high_str, "%lf", &target_high);
		if (ret == 1)
		{
			g_debug("J_BENCHMARK_TARGET_HIGH %s %f", target_high_str, target_high);
			target_time_high = target_high;
		}
	}
	all_result_step = g_new0(result_step, 2);
	current_result_step = all_result_step;
	next_result_step = all_result_step + 1;
	j_benchmark_timer = g_timer_new();
	n = world_size;
	current_result_step->n = n;
	while (1)
	{
		n_next = n * multiplicator;
		exec_tests(n);
		fflush(stdout);
		n = n_next;
		if (!calculate_prognose(n_next))
			break;
		fflush(stdout);
		tmp = current_result_step;
		current_result_step = next_result_step;
		next_result_step = tmp;
	}
	fflush(stdout);
	g_timer_destroy(j_benchmark_timer);
	g_free(all_result_step);
}
#endif
