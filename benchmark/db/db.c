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

#include <julea-config.h>
#include <math.h>
#include <glib.h>
#include <string.h>
#include <julea-db.h>
#include <julea.h>
#include "benchmark.h"
#include <julea-internal.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef JULEA_DEBUG
#define ERROR_PARAM &error
#define CHECK_ERROR(_ret_)                                                       \
	do                                                                       \
	{                                                                        \
		if (error)                                                       \
		{                                                                \
			J_DEBUG("ERROR (%d) (%s)", error->code, error->message); \
			abort();                                                 \
		}                                                                \
		if (_ret_)                                                       \
		{                                                                \
			J_DEBUG("ret was %d", _ret_);                            \
			abort();                                                 \
		}                                                                \
	} while (0)
static gdouble target_time = 1;
#else
#define ERROR_PARAM NULL
#define CHECK_ERROR(_ret_)   \
	do                   \
	{                    \
		(void)error; \
		(void)_ret_; \
	} while (0)
static gdouble target_time = 60;
#endif

static guint n = 1;
static JDBSchema** schema_array;

static void
_benchmark_db_schema_create(BenchmarkResult* result, gboolean use_batch)
{
	GError* error = NULL;
	gboolean ret;
	const char* namespace = "namespace";
	char name[50];
	guint i;
	guint m = 0;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed = 0;
	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);
start:
	m++;
	j_benchmark_timer_start();
	for (i = 0; i < n; i++)
	{
		sprintf(name, "name%d", i);
		schema_array[i] = j_db_schema_new(namespace, name, ERROR_PARAM);
		CHECK_ERROR(!schema_array[i]);
		ret = j_db_schema_add_field(schema_array[i], "name", J_DB_TYPE_STRING, ERROR_PARAM);
		CHECK_ERROR(!ret);
		ret = j_db_schema_add_field(schema_array[i], "loc", J_DB_TYPE_UINT32, ERROR_PARAM);
		CHECK_ERROR(!ret);
		ret = j_db_schema_add_field(schema_array[i], "coverage", J_DB_TYPE_FLOAT32, ERROR_PARAM);
		CHECK_ERROR(!ret);
		ret = j_db_schema_add_field(schema_array[i], "lastrun", J_DB_TYPE_UINT32, ERROR_PARAM);
		CHECK_ERROR(!ret);
		ret = j_db_schema_create(schema_array[i], batch, ERROR_PARAM);
		CHECK_ERROR(!ret);
		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			CHECK_ERROR(!ret);
		}
	}
	if (use_batch)
	{
		ret = j_batch_execute(batch);
		CHECK_ERROR(!ret);
	}
	elapsed += j_benchmark_timer_elapsed();
	if (elapsed < target_time)
	{
		for (i = 0; i < n; i++)
		{
			ret = j_db_schema_delete(schema_array[i], batch, ERROR_PARAM);
			CHECK_ERROR(!ret);
		}
		ret = j_batch_execute(batch);
		CHECK_ERROR(!ret);
		for (i = 0; i < n; i++)
			j_db_schema_unref(schema_array[i]);
		goto start;
	}
	result->elapsed_time = elapsed;
	result->operations = n * m;
}
static void
_benchmark_db_schema_delete(BenchmarkResult* result, gboolean use_batch)
{
	GError* error = NULL;
	gboolean ret;
	const char* namespace = "namespace";
	char name[50];
	guint i;
	guint m = 0;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed = 0;
	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);
start:
	m++;
	j_benchmark_timer_start();
	for (i = 0; i < n; i++)
	{
		ret = j_db_schema_delete(schema_array[i], batch, ERROR_PARAM);
		CHECK_ERROR(!ret);
		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			CHECK_ERROR(!ret);
		}
	}
	if (use_batch)
	{
		ret = j_batch_execute(batch);
		CHECK_ERROR(!ret);
	}
	elapsed += j_benchmark_timer_elapsed();
	if (elapsed < target_time)
	{
		for (i = 0; i < n; i++)
		{
			j_db_schema_unref(schema_array[i]);
			sprintf(name, "name%d", i);
			schema_array[i] = j_db_schema_new(namespace, name, ERROR_PARAM);
			CHECK_ERROR(!schema_array[i]);
			ret = j_db_schema_add_field(schema_array[i], "name", J_DB_TYPE_STRING, ERROR_PARAM);
			CHECK_ERROR(!ret);
			ret = j_db_schema_add_field(schema_array[i], "loc", J_DB_TYPE_UINT32, ERROR_PARAM);
			CHECK_ERROR(!ret);
			ret = j_db_schema_add_field(schema_array[i], "coverage", J_DB_TYPE_FLOAT32, ERROR_PARAM);
			CHECK_ERROR(!ret);
			ret = j_db_schema_add_field(schema_array[i], "lastrun", J_DB_TYPE_UINT32, ERROR_PARAM);
			CHECK_ERROR(!ret);
			ret = j_db_schema_create(schema_array[i], batch, ERROR_PARAM);
			CHECK_ERROR(!ret);
		}
		ret = j_batch_execute(batch);
		CHECK_ERROR(!ret);
		goto start;
	}
	for (i = 0; i < n; i++)
		j_db_schema_unref(schema_array[i]);
	result->elapsed_time = elapsed;
	result->operations = n * m;
}
static void
_benchmark_db_schema_get(BenchmarkResult* result, gboolean use_batch)
{
	GError* error = NULL;
	gboolean ret;
	const char* namespace = "namespace";
	char name[50];
	guint i;
	guint m = 0;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed = 0;
	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);
start:
	for (i = 0; i < n; i++)
		j_db_schema_unref(schema_array[i]);
	for (i = 0; i < n; i++)
	{
		sprintf(name, "name%d", i);
		schema_array[i] = j_db_schema_new(namespace, name, ERROR_PARAM);
		CHECK_ERROR(!schema_array[i]);
	}
	m++;
	j_benchmark_timer_start();
	for (i = 0; i < n; i++)
	{
		ret = j_db_schema_get(schema_array[i], batch, ERROR_PARAM);
		CHECK_ERROR(!ret);
		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			CHECK_ERROR(!ret);
		}
	}
	if (use_batch)
	{
		ret = j_batch_execute(batch);
		CHECK_ERROR(!ret);
	}
	elapsed += j_benchmark_timer_elapsed();
	if (elapsed < target_time)
	{
		goto start;
	}
	result->elapsed_time = elapsed;
	result->operations = n * m;
}
static void
_benchmark_db_entry_insert(BenchmarkResult* result, gboolean use_batch)
{
	GError* error = NULL;
	gboolean ret;
	const char* namespace = "namespace";
	const char* name = "name";
	guint array_length = 100;
	JDBEntry** entry;
	JDBSchema* schema;
	guint i;
	guint j;
	char varname[50];
	guint m = 0;
	g_autoptr(JBatch) batch = NULL;
	gdouble elapsed = 0;
	g_autoptr(JSemantics) semantics = NULL;
	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);
	entry = g_new(JDBEntry*, n);
start:
	schema = j_db_schema_new(namespace, name, ERROR_PARAM);
	CHECK_ERROR(!schema);
	for (i = 0; i < array_length; i++)
	{
		sprintf(varname, "varname_%d", i);
		ret = j_db_schema_add_field(schema, varname, J_DB_TYPE_UINT32, ERROR_PARAM);
		CHECK_ERROR(!ret);
	}
	ret = j_db_schema_create(schema, batch, ERROR_PARAM);
	CHECK_ERROR(!ret);
	ret = j_batch_execute(batch);
	CHECK_ERROR(!ret);
	m++;
	j_benchmark_timer_start();
	for (j = 0; j < n; j++)
	{
		entry[j] = j_db_entry_new(schema, ERROR_PARAM);
		CHECK_ERROR(!entry);
		for (i = 0; i < array_length; i++)
		{
			sprintf(varname, "varname_%d", i);
			ret = j_db_entry_set_field(entry[j], varname, &j, 4, ERROR_PARAM);
			CHECK_ERROR(!ret);
		}
		ret = j_db_entry_insert(entry[j], batch, ERROR_PARAM);
		CHECK_ERROR(!ret);
		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			CHECK_ERROR(!ret);
		}
	}
	if (use_batch)
	{
		ret = j_batch_execute(batch);
		CHECK_ERROR(!ret);
	}
	elapsed += j_benchmark_timer_elapsed();
	ret = j_db_schema_delete(schema, batch, ERROR_PARAM);
	CHECK_ERROR(!ret);
	ret = j_batch_execute(batch);
	CHECK_ERROR(!ret);
	j_db_schema_unref(schema);
	for (j = 0; j < n; j++)
		j_db_entry_unref(entry[j]);
	if (elapsed < target_time)
		goto start;
	result->elapsed_time = elapsed;
	result->operations = n * m;
	g_free(entry);
}
static void
benchmark_db_schema_create(BenchmarkResult* result)
{
	_benchmark_db_schema_create(result, FALSE);
}
static void
benchmark_db_schema_create_batch(BenchmarkResult* result)
{
	_benchmark_db_schema_create(result, TRUE);
}
static void
benchmark_db_schema_delete(BenchmarkResult* result)
{
	_benchmark_db_schema_delete(result, FALSE);
}
static void
benchmark_db_schema_delete_batch(BenchmarkResult* result)
{
	_benchmark_db_schema_delete(result, TRUE);
}
static void
benchmark_db_schema_get(BenchmarkResult* result)
{
	_benchmark_db_schema_get(result, FALSE);
}
static void
benchmark_db_schema_get_batch(BenchmarkResult* result)
{
	_benchmark_db_schema_get(result, TRUE);
}
static void
benchmark_db_entry_insert(BenchmarkResult* result)
{
	_benchmark_db_entry_insert(result, FALSE);
}
static void
benchmark_db_entry_insert_batch(BenchmarkResult* result)
{
	_benchmark_db_entry_insert(result, TRUE);
}
static void
exec_tests()
{
	char testname[500];
	if (n <= 100000)
	{
		//tests with more than 100000 schema does not make sense
		schema_array = g_new(JDBSchema*, n);
		if (n <= 1000)
		{
			//not using batches with more than 1000 same functions does not make sense
			sprintf(testname, "/db/%d/schema/create", n);
			j_benchmark_run(testname, benchmark_db_schema_create);
			sprintf(testname, "/db/%d/schema/get", n);
			j_benchmark_run(testname, benchmark_db_schema_get);
			sprintf(testname, "/db/%d/schema/delete", n);
			j_benchmark_run(testname, benchmark_db_schema_delete);
		}
		sprintf(testname, "/db/%d/schema/create-batch", n);
		j_benchmark_run(testname, benchmark_db_schema_create_batch);
		sprintf(testname, "/db/%d/schema/get-batch", n);
		j_benchmark_run(testname, benchmark_db_schema_get_batch);
		sprintf(testname, "/db/%d/schema/delete-batch", n);
		j_benchmark_run(testname, benchmark_db_schema_delete_batch);
		g_free(schema_array);
	}
	if (n <= 1000)
	{
		//not using batches with more than 1000 same functions does not make sense
		sprintf(testname, "/db/%d/entry/insert", n);
		j_benchmark_run(testname, benchmark_db_entry_insert);
	}
	sprintf(testname, "/db/%d/entry/insert-batch", n);
	j_benchmark_run(testname, benchmark_db_entry_insert_batch);
}
static void
exec_tree(guint depth, gfloat min, gfloat max)
{
	//exec tests such that n increases exponentially
	//exec tests in an ordering such that some huge and some small n are executed fast to gain a overview of the result before executing everything completely
	gfloat val = (max - min) * 0.5 + min;
	guint imin = pow(min, 10.0);
	guint imax = pow(max, 10.0);
	guint ival = pow(val, 10.0);
	if (ival != imin && ival != imax)
	{
		if (depth == 0)
		{
			n = ival;
			exec_tests();
		}
		else
		{
			exec_tree(depth - 1, min, val);
			exec_tree(depth - 1, val, max);
		}
	}
}
static void
exec_tree1(guint depth, gfloat min, gfloat max)
{
	if (depth == 0)
	{
		n = min;
		exec_tests();
		n = max;
		exec_tests();
	}
	exec_tree(depth, pow(min, 1.0 / 10.0), pow(max, 1.0 / 10.0));
}
void
benchmark_db(void)
{
#ifdef JULEA_DEBUG
	exec_tree1(0, 1, 5);
#else
	guint i;
	for (i = 0; i < 7; i++)
		exec_tree1(i, 1, 1000000);
#endif
}
