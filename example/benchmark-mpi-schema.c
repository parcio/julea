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

#ifndef BENCHMARK_MPI_MAIN_C
//compile for code formatting reasons
#include "benchmark-mpi-main.c"
#endif

#ifndef BENCHMARK_MPI_SCHEMA_C
#define BENCHMARK_MPI_SCHEMA_C

#include "benchmark-mpi.h"

static
void
_benchmark_db_schema_create(gboolean use_batch, const guint n)
{
	J_TRACE_FUNCTION(NULL);

	JDBSchema** schema_array = NULL;
	GError* error = NULL;
	gboolean ret;
	char name[50];
	guint i;
	guint m = 0;
	guint m2 = 0;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;

	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	batch = j_batch_new(semantics);
	schema_array = g_new0(JDBSchema*, n);
	if (current_result_step->schema_create[use_batch].prognosted_time < target_time_high && current_result_step->schema_delete[use_batch].prognosted_time < target_time_high)
		while (m == 0 || (current_result_step->schema_create[use_batch].elapsed_time < target_time_low && current_result_step->schema_delete[use_batch].elapsed_time < target_time_low))
		{
			m++;
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
			}
			j_benchmark_timer_start();
			for (i = 0; i < n; i++)
			{
				sprintf(name, "name%d", i);
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
			current_result_step->schema_create[use_batch].elapsed_time += j_benchmark_timer_elapsed();
			if (current_result_step->schema_get[use_batch].prognosted_time < target_time_high)
				while (m2 == 0 || current_result_step->schema_get[use_batch].elapsed_time < target_time_low)
				{
					for (i = 0; i < n; i++)
					{
						j_db_schema_unref(schema_array[i]);
					}
					for (i = 0; i < n; i++)
					{
						sprintf(name, "name%d", i);
						schema_array[i] = j_db_schema_new(namespace, name, ERROR_PARAM);
						CHECK_ERROR(!schema_array[i]);
					}
					m2++;
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
					current_result_step->schema_get[use_batch].elapsed_time += j_benchmark_timer_elapsed();
				}
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
			current_result_step->schema_delete[use_batch].elapsed_time += j_benchmark_timer_elapsed();
			for (i = 0; i < n; i++)
			{
				j_db_schema_unref(schema_array[i]);
			}
		}
	g_free(schema_array);

	current_result_step->schema_create[use_batch].operations = n * m;
	current_result_step->schema_get[use_batch].operations = n * m2;
	current_result_step->schema_delete[use_batch].operations = n * m;
	current_result_step->schema_create[use_batch].operations_without_n = m;
	current_result_step->schema_get[use_batch].operations_without_n = m2;
	current_result_step->schema_delete[use_batch].operations_without_n = m;
}
static
void
_benchmark_db_schema_ref(void)
{
	J_TRACE_FUNCTION(NULL);

	guint batch_count = 1000;
	GError* error = NULL;
	const char* name = "name";
	JDBSchema* schema;
	JDBSchema* schema2;
	gboolean ret;
	guint i;
	guint m = 0;

	schema = j_db_schema_new(namespace, name, ERROR_PARAM);
	CHECK_ERROR(!schema);
	if (current_result_step->schema_ref.prognosted_time < target_time_high && current_result_step->schema_unref.prognosted_time < target_time_high)
		while (m == 0 || (current_result_step->schema_ref.elapsed_time < target_time_low && current_result_step->schema_unref.elapsed_time < target_time_low))
		{
			m += batch_count;
			j_benchmark_timer_start();
			for (i = 0; i < batch_count; i++)
			{
				schema2 = j_db_schema_ref(schema);
				CHECK_ERROR(!schema2);
				ret = schema != schema2;
				CHECK_ERROR(ret);
			}
			current_result_step->schema_ref.elapsed_time += j_benchmark_timer_elapsed();
			j_benchmark_timer_start();
			for (i = 0; i < batch_count; i++)
			{
				j_db_schema_unref(schema);
			}
			current_result_step->schema_unref.elapsed_time += j_benchmark_timer_elapsed();
		}
	j_db_schema_unref(schema);
	current_result_step->schema_ref.operations = m;
	current_result_step->schema_unref.operations = m;
	current_result_step->schema_ref.operations_without_n = m;
	current_result_step->schema_unref.operations_without_n = m;
}
static
void
_benchmark_db_schema_new(void)
{
	J_TRACE_FUNCTION(NULL);

	guint batch_count = 1000;
	GError* error = NULL;
	const char* name = "name";
	guint i;
	JDBSchema** schema_array = NULL;
	guint m = 0;

	schema_array = g_new(JDBSchema*, batch_count);
	if (current_result_step->schema_new.prognosted_time < target_time_high && current_result_step->schema_free.prognosted_time < target_time_high)
		while (m == 0 || (current_result_step->schema_new.elapsed_time < target_time_low && current_result_step->schema_free.elapsed_time < target_time_low))
		{
			m += batch_count;
			j_benchmark_timer_start();
			for (i = 0; i < batch_count; i++)
			{
				schema_array[i] = j_db_schema_new(namespace, name, ERROR_PARAM);
				CHECK_ERROR(!schema_array[i]);
			}
			current_result_step->schema_new.elapsed_time += j_benchmark_timer_elapsed();
			j_benchmark_timer_start();
			for (i = 0; i < batch_count; i++)
			{
				j_db_schema_unref(schema_array[i]);
			}
			current_result_step->schema_free.elapsed_time += j_benchmark_timer_elapsed();
		}
	g_free(schema_array);

	current_result_step->schema_new.operations = m;
	current_result_step->schema_free.operations = m;
	current_result_step->schema_new.operations_without_n = m;
	current_result_step->schema_free.operations_without_n = m;
}
static
void
_benchmark_db_schema_add_field(const guint n)
{
	J_TRACE_FUNCTION(NULL);

	gchar** names;
	JDBType* types;
	char varname[50];
	GError* error = NULL;
	const char* name = "name";
	JDBSchema* schema = NULL;
	JDBSchema* schema2;
	gboolean equals;
	guint i;
	guint j;
	gboolean ret;
	guint m = 0;
	guint m2 = 0;
	guint m3 = 0;
	guint m4 = 0;
	JDBType type;
	if (current_result_step->schema_add_field.prognosted_time < target_time_high)
		while (m == 0 || current_result_step->schema_add_field.elapsed_time < target_time_low)
		{
			if (schema)
			{
				j_db_schema_unref(schema);
			}
			schema = j_db_schema_new(namespace, name, ERROR_PARAM);
			CHECK_ERROR(!schema);
			m++;
			j_benchmark_timer_start();
			for (i = 0; i < n; i++)
			{
				sprintf(varname, "varname_%d", i);
				ret = j_db_schema_add_field(schema, varname, J_DB_TYPE_UINT32, ERROR_PARAM);
				CHECK_ERROR(!ret);
			}
			current_result_step->schema_add_field.elapsed_time += j_benchmark_timer_elapsed();
			if (current_result_step->schema_get_field.prognosted_time < target_time_high)
				while (m2 == 0 || current_result_step->schema_get_field.elapsed_time < target_time_low)
				{
					m2++;
					j_benchmark_timer_start();
					for (i = 0; i < n; i++)
					{
						sprintf(varname, "varname_%d", i);
						ret = j_db_schema_get_field(schema, varname, &type, ERROR_PARAM);
						CHECK_ERROR(!ret);
					}
					current_result_step->schema_get_field.elapsed_time += j_benchmark_timer_elapsed();
				}
			if (current_result_step->schema_get_fields.prognosted_time < target_time_high)
				while (m3 == 0 || current_result_step->schema_get_fields.elapsed_time < target_time_low)
				{
					m3++;
					j_benchmark_timer_start();
					sprintf(varname, "varname_%d", i);
					j = j_db_schema_get_all_fields(schema, &names, &types, ERROR_PARAM);
					CHECK_ERROR(!j);
					current_result_step->schema_get_fields.elapsed_time += j_benchmark_timer_elapsed();
					g_strfreev(names);
					g_free(types);
				}
		}
	schema2 = j_db_schema_new(namespace, name, ERROR_PARAM);
	CHECK_ERROR(!schema2);
	m++;
	for (i = 0; i < n; i++)
	{
		sprintf(varname, "varname_%d", i);
		ret = j_db_schema_add_field(schema2, varname, J_DB_TYPE_UINT32, ERROR_PARAM);
		CHECK_ERROR(!ret);
	}
	if (current_result_step->schema_equals.prognosted_time < target_time_high)
		while (m4 == 0 || current_result_step->schema_equals.elapsed_time < target_time_low)
		{
			m4++;
			j_benchmark_timer_start();
			ret = j_db_schema_equals(schema, schema2, &equals, ERROR_PARAM);
			CHECK_ERROR(!ret);
			current_result_step->schema_equals.elapsed_time += j_benchmark_timer_elapsed();
		}
	j_db_schema_unref(schema);
	j_db_schema_unref(schema2);
	current_result_step->schema_add_field.operations = n * m;
	current_result_step->schema_get_field.operations = n * m2;
	current_result_step->schema_get_fields.operations = n * m3;
	current_result_step->schema_equals.operations = n * m4;
	current_result_step->schema_add_field.operations_without_n = m;
	current_result_step->schema_get_field.operations_without_n = m2;
	current_result_step->schema_get_fields.operations_without_n = m3;
	current_result_step->schema_equals.operations_without_n = m4;
}
#endif
