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

#ifndef BENCHMARK_MPI_ENTRY_C
#define BENCHMARK_MPI_ENTRY_C

#include "benchmark-mpi.h"

struct JBatch
{
	/**
         * The list of pending operations.
         **/
	JList* list;

	/**
         * The semantics.
         **/
	JSemantics* semantics;

	/**
         * The background operation used for j_batch_execute_async().
         **/
	JBackgroundOperation* background_operation;

	/**
         * The reference count.
         **/
	gint ref_count;
};

static
void
_benchmark_db_entry_ref(void)
{
	J_TRACE_FUNCTION(NULL);

	guint batch_count = 1000;
	GError* error = NULL;
	const char* name = "name";
	JDBSchema* schema;
	JDBEntry* entry;
	JDBEntry* entry2;
	gboolean ret;
	guint i;
	guint m = 0;

	schema = j_db_schema_new(namespace, name, ERROR_PARAM);
	CHECK_ERROR(!schema);
	entry = j_db_entry_new(schema, ERROR_PARAM);
	CHECK_ERROR(!entry);
	if (current_result_step->entry_ref.prognosted_time < target_time_high && current_result_step->entry_unref.prognosted_time < target_time_high)
		while ((current_result_step->entry_ref.elapsed_time < target_time_low && current_result_step->entry_unref.elapsed_time < target_time_low))
		{
			m += batch_count;
			j_benchmark_timer_start();
			for (i = 0; i < batch_count; i++)
			{
				entry2 = j_db_entry_ref(entry);
				CHECK_ERROR(!entry2);
				ret = entry != entry2;
				CHECK_ERROR(ret);
			}
			current_result_step->entry_ref.elapsed_time += j_benchmark_timer_elapsed();
			j_benchmark_timer_start();
			for (i = 0; i < batch_count; i++)
			{
				j_db_entry_unref(entry);
			}
			current_result_step->entry_unref.elapsed_time += j_benchmark_timer_elapsed();
		}
	j_db_entry_unref(entry);
	j_db_schema_unref(schema);
	current_result_step->entry_ref.operations = m;
	current_result_step->entry_unref.operations = m;
	current_result_step->entry_ref.operations_without_n = m;
	current_result_step->entry_unref.operations_without_n = m;
}
static
void
_benchmark_db_entry_new(void)
{
	J_TRACE_FUNCTION(NULL);

	guint batch_count = 1000;
	GError* error = NULL;
	const char* name = "name";
	guint i;
	JDBSchema* schema;
	JDBEntry** entry_array = NULL;
	guint m = 0;

	entry_array = g_new(JDBEntry*, batch_count);
	schema = j_db_schema_new(namespace, name, ERROR_PARAM);
	CHECK_ERROR(!schema);
	if (current_result_step->entry_new.prognosted_time < target_time_high && current_result_step->entry_free.prognosted_time < target_time_high)
		while ((current_result_step->entry_new.elapsed_time < target_time_low && current_result_step->entry_free.elapsed_time < target_time_low))
		{
			m += batch_count;
			j_benchmark_timer_start();
			for (i = 0; i < batch_count; i++)
			{
				entry_array[i] = j_db_entry_new(schema, ERROR_PARAM);
				CHECK_ERROR(!entry_array[i]);
			}
			current_result_step->entry_new.elapsed_time += j_benchmark_timer_elapsed();
			j_benchmark_timer_start();
			for (i = 0; i < batch_count; i++)
			{
				j_db_entry_unref(entry_array[i]);
			}
			current_result_step->entry_free.elapsed_time += j_benchmark_timer_elapsed();
		}
	g_free(entry_array);
	j_db_schema_unref(schema);
	current_result_step->entry_new.operations = m;
	current_result_step->entry_free.operations = m;
	current_result_step->entry_new.operations_without_n = m;
	current_result_step->entry_free.operations_without_n = m;
}
static
void
_benchmark_db_entry_set_field(const guint n)
{
	J_TRACE_FUNCTION(NULL);

	GError* error = NULL;
	gboolean ret;
	const char* name = "name";
	JDBEntry* entry;
	JDBSchema* schema;
	guint i;
	char varname[50];
	guint m = 0;

	schema = j_db_schema_new(namespace, name, ERROR_PARAM);
	CHECK_ERROR(!schema);
	for (i = 0; i < n; i++)
	{
		sprintf(varname, "varname_%d", i);
		ret = j_db_schema_add_field(schema, varname, J_DB_TYPE_UINT32, ERROR_PARAM);
		CHECK_ERROR(!ret);
	}
	if (current_result_step->entry_set_field.prognosted_time < target_time_high)
		while (current_result_step->entry_set_field.elapsed_time < target_time_low)
		{
			m++;
			entry = j_db_entry_new(schema, ERROR_PARAM);
			CHECK_ERROR(!entry);
			j_benchmark_timer_start();
			for (i = 0; i < n; i++)
			{
				sprintf(varname, "varname_%d", i);
				ret = j_db_entry_set_field(entry, varname, &i, 4, ERROR_PARAM);
				CHECK_ERROR(!ret);
			}
			current_result_step->entry_set_field.elapsed_time += j_benchmark_timer_elapsed();
			j_db_entry_unref(entry);
		}
	j_db_schema_unref(schema);
	current_result_step->entry_set_field.operations = m;
	current_result_step->entry_set_field.operations_without_n = m;
}
static
void
_benchmark_db_entry_insert(gboolean use_batch, gboolean use_index, const guint n1, const guint n2, JSemanticsAtomicity atomicity)
{
	J_TRACE_FUNCTION(NULL);

	GError* error = NULL;
	GError** _error = NULL;
	gboolean ret;
	gpointer iter_ptr;
	JDBType iter_type;
	guint64 iter_length;
	const char* name = "name";
	JDBEntry** entry;
	JDBSelector** selector;
	JDBSchema* schema;
	JDBIterator* iterator;
	guint i;
	guint k;
	guint j;
	guint j2;
	gchar const** names;
	char varname[50];
	guint m = 0;
	guint m2 = 0;
	guint m3 = 0;
	guint m4 = 0;
	guint m5 = 0;
	guint m_tmp;
	const guint n_local = n1 / world_size; //assert (n_1 % world_size == 0)
	const guint n_off = n_local * world_rank;
	gboolean allow_loop;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gboolean batch_empty = TRUE;

	guint my_index;
	if (!use_batch)
		my_index = 0;
	else if (!use_index)
		my_index = 1;
	else if (atomicity == J_SEMANTICS_ATOMICITY_NONE)
		my_index = 2;
	else if (atomicity == J_SEMANTICS_ATOMICITY_BATCH)
		my_index = 3;
	//else error
	if (n2 == 50)
		my_index += 4;
	else if (n2 == 500)
		my_index += 8;

	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	j_semantics_set(semantics, J_SEMANTICS_ATOMICITY, atomicity);
	batch = j_batch_new(semantics);
	entry = g_new(JDBEntry*, n_local);
	selector = g_new(JDBSelector*, n_local);
	_error = g_new0(GError*, n_local);
	names = g_new(gchar const*, 2);
	names[0] = "varname_0";
	names[1] = NULL;
_start:
	MPI_Barrier(MPI_COMM_WORLD);
	allow_loop = FALSE;
	schema = j_db_schema_new("namespace", name, ERROR_PARAM);
	CHECK_ERROR(!schema);
	if (world_rank == 0)
	{
		for (i = 0; i < n2; i++)
		{
			sprintf(varname, "varname_%d", i);
			ret = j_db_schema_add_field(schema, varname, J_DB_TYPE_UINT32, ERROR_PARAM);
			CHECK_ERROR(!ret);
		}
		if (use_index)
		{
			ret = j_db_schema_add_index(schema, names, ERROR_PARAM);
			CHECK_ERROR(!ret);
		}
		ret = j_db_schema_create(schema, batch, ERROR_PARAM);
		CHECK_ERROR(!ret);
		ret = j_batch_execute(batch);
		CHECK_ERROR(!ret);
		j_db_schema_unref(schema);
		schema = j_db_schema_new("namespace", name, ERROR_PARAM);
		CHECK_ERROR(!schema);
	}
	MPI_Barrier(MPI_COMM_WORLD);
	ret = j_db_schema_get(schema, batch, ERROR_PARAM);
	CHECK_ERROR(!ret);
	ret = j_batch_execute(batch);
	CHECK_ERROR(!ret);
	MPI_Barrier(MPI_COMM_WORLD);
	if (current_result_step->entry_insert[my_index].prognosted_time < target_time_high)
	{
		while (current_result_step->entry_insert[my_index].elapsed_time < target_time_low)
		{
			allow_loop = TRUE;
			m++;
			//insert
			j_benchmark_timer_start();
			for (j = 0; j < n_local; j++)
			{
				j2 = j + n_off;
				entry[j] = j_db_entry_new(schema, ERROR_PARAM);
				CHECK_ERROR(!entry[j]);
				for (i = 0; i < n2; i++)
				{
					sprintf(varname, "varname_%d", i);
					ret = j_db_entry_set_field(entry[j], varname, &j2, 4, ERROR_PARAM);
					CHECK_ERROR(!ret);
				}
				ret = j_db_entry_insert(entry[j], batch, ERROR_PARAM2(j));
				batch_empty = FALSE;
				CHECK_ERROR(!ret);
				if (!use_batch || ((j % batch_size) == 0))
				{
					ret = j_batch_execute(batch);
					batch_empty = TRUE;
					CHECK_ERROR2(!ret);
					current_result_step->entry_insert[my_index].elapsed_time += j_benchmark_timer_elapsed();
					j_benchmark_timer_start();
				}
				if (current_result_step->entry_insert[my_index].elapsed_time > target_time_high && m == 1 && ((((gdouble)j) / ((gdouble)n_local)) < allowed_percentage))
				{
					m = 0;
					allow_loop = FALSE;
					for (k = 0; k <= j; k++)
					{
						j_db_entry_unref(entry[k]);
					}
					goto _abort;
				}
			}
			if (use_batch && !batch_empty)
			{
				ret = j_batch_execute(batch);
				batch_empty = TRUE;
				CHECK_ERROR2(!ret);
				current_result_step->entry_insert[my_index].elapsed_time += j_benchmark_timer_elapsed();
			}
			for (j = 0; j < n_local; j++)
			{
				j_db_entry_unref(entry[j]);
			}
			if (use_batch)
			{
				//selector single
				if (current_result_step->iterator_single[my_index].prognosted_time < target_time_high)
				{
					while (current_result_step->iterator_single[my_index].elapsed_time < target_time_low)
					{
						m3++;
						for (j = 0; j < n_local; j++)
						{
							j2 = j + n_off;
							j_benchmark_timer_start();
							selector[j] = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, ERROR_PARAM);
							CHECK_ERROR(!selector);
							ret = j_db_selector_add_field(selector[j], varname, J_DB_SELECTOR_OPERATOR_EQ, &j2, 4, ERROR_PARAM);
							CHECK_ERROR(!ret);
							iterator = j_db_iterator_new(schema, selector[j], ERROR_PARAM);
							CHECK_ERROR(!iterator);
							ret = j_db_iterator_next(iterator, ERROR_PARAM);
							CHECK_ERROR(!ret);
							for (i = 0; i < n2; i++)
							{
								sprintf(varname, "varname_%d", i);
								iter_ptr = NULL;
								ret = j_db_iterator_get_field(iterator, varname, &iter_type, &iter_ptr, &iter_length, ERROR_PARAM);
								CHECK_ERROR(!ret);
								g_free(iter_ptr);
							}
							ret = j_db_iterator_next(iterator, NULL);
							CHECK_ERROR(ret);
							j_db_iterator_unref(iterator);
							j_db_selector_unref(selector[j]);
							current_result_step->iterator_single[my_index].elapsed_time += j_benchmark_timer_elapsed();
							if (current_result_step->iterator_single[my_index].elapsed_time > target_time_high && m3 == 1 && ((((gdouble)j) / ((gdouble)n_local)) < allowed_percentage))
							{
								m3 = 0;
								goto _selector_single_end;
							}
						}
					}
				}
			_selector_single_end:
				//selector all
				if (current_result_step->iterator_all[my_index].prognosted_time < target_time_high)
				{
					while (current_result_step->iterator_all[my_index].elapsed_time < target_time_low)
					{
						j_benchmark_timer_start();
						if (world_rank == 0)
						{
							iterator = j_db_iterator_new(schema, NULL, ERROR_PARAM);
							CHECK_ERROR(!iterator);
							while (j_db_iterator_next(iterator, NULL))
							{
								m4++;
								for (i = 0; i < n2; i++)
								{
									sprintf(varname, "varname_%d", i);
									ret = j_db_iterator_get_field(iterator, varname, &iter_type, &iter_ptr, &iter_length, ERROR_PARAM);
									CHECK_ERROR(!ret);
									g_free(iter_ptr);
								}
							}
							j_db_iterator_unref(iterator);
						}
						current_result_step->iterator_all[my_index].elapsed_time += j_benchmark_timer_elapsed();
						{
							//only master selects all - distribute m4 because otherwise all other processes wont wait for master
							MPI_Allreduce(&m4, &m_tmp, 1, MPI_UNSIGNED, MPI_MAX, MPI_COMM_WORLD);
							m4 = m_tmp;
						}
					}
				}
			}
			if (current_result_step->entry_update[my_index].prognosted_time < target_time_high)
			{
				while (current_result_step->entry_update[my_index].elapsed_time < target_time_low)
				{
					m2++;
					//update
					j_benchmark_timer_start();
					for (j = 0; j < n_local; j++)
					{
						j2 = j + n_off;
						entry[j] = j_db_entry_new(schema, ERROR_PARAM);
						CHECK_ERROR(!entry[j]);
						for (i = 1; i < n2; i++)
						{
							sprintf(varname, "varname_%d", i);
							ret = j_db_entry_set_field(entry[j], varname, &j2, 4, ERROR_PARAM);
							CHECK_ERROR(!ret);
						}
						selector[j] = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, ERROR_PARAM);
						CHECK_ERROR(!selector);
						ret = j_db_selector_add_field(selector[j], "varname_0", J_DB_SELECTOR_OPERATOR_EQ, &j2, 4, ERROR_PARAM);
						CHECK_ERROR(!ret);
						ret = j_db_entry_update(entry[j], selector[j], batch, ERROR_PARAM2(j));
						batch_empty = FALSE;
						CHECK_ERROR(!ret);
						if (!use_batch || ((j % batch_size) == 0))
						{
							ret = j_batch_execute(batch);
							batch_empty = TRUE;
							CHECK_ERROR2(!ret);
							current_result_step->entry_update[my_index].elapsed_time += j_benchmark_timer_elapsed();
							j_benchmark_timer_start();
						}
						if (current_result_step->entry_update[my_index].elapsed_time > target_time_high && m2 == 1 && ((((gdouble)j) / ((gdouble)n_local)) < allowed_percentage))
						{
							m2 = 0;
							for (k = 0; k <= j; k++)
							{
								j_db_entry_unref(entry[k]);
								j_db_selector_unref(selector[k]);
							}
							goto _selector_all_end;
						}
					}
					if (use_batch && !batch_empty)
					{
						ret = j_batch_execute(batch);
						batch_empty = TRUE;
						CHECK_ERROR2(!ret);
						current_result_step->entry_update[my_index].elapsed_time += j_benchmark_timer_elapsed();
					}
					for (j = 0; j < n_local; j++)
					{
						j_db_entry_unref(entry[j]);
						j_db_selector_unref(selector[j]);
					}
				}
			}
		_selector_all_end:
			//delete
			if (current_result_step->entry_delete[my_index].prognosted_time < target_time_high)
			{
				if (current_result_step->entry_delete[my_index].elapsed_time < target_time_low)
				{
					m5++;
					j_benchmark_timer_start();
					for (j = 0; j < n_local; j++)
					{
						j2 = j + n_off;
						CHECK_ERROR(!entry);
						selector[j] = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, ERROR_PARAM);
						CHECK_ERROR(!selector[j]);
						ret = j_db_selector_add_field(selector[j], "varname_0", J_DB_SELECTOR_OPERATOR_EQ, &j2, 4, ERROR_PARAM);
						CHECK_ERROR(!ret);
						entry[j] = j_db_entry_new(schema, ERROR_PARAM);
						CHECK_ERROR(!entry[j]);
						ret = j_db_entry_delete(entry[j], selector[j], batch, ERROR_PARAM2(j));
						batch_empty = FALSE;
						CHECK_ERROR(!ret);
						if (!use_batch || ((j % batch_size) == 0))
						{
							ret = j_batch_execute(batch);
							batch_empty = TRUE;
							CHECK_ERROR2(!ret);
							current_result_step->entry_delete[my_index].elapsed_time += j_benchmark_timer_elapsed();
							j_benchmark_timer_start();
						}
						if (current_result_step->entry_delete[my_index].elapsed_time > target_time_high && m5 == 1 && ((((gdouble)j) / ((gdouble)n_local)) < allowed_percentage))
						{
							m5 = 0;
							for (k = 0; k <= j; k++)
							{
								j_db_entry_unref(entry[k]);
								j_db_selector_unref(selector[k]);
							}
							goto _abort;
						}
					}
					if (use_batch && !batch_empty)
					{
						ret = j_batch_execute(batch);
						batch_empty = TRUE;
						CHECK_ERROR2(!ret);
						current_result_step->entry_delete[my_index].elapsed_time += j_benchmark_timer_elapsed();
					}
					for (j = 0; j < n_local; j++)
					{
						j_db_entry_unref(entry[j]);
						j_db_selector_unref(selector[j]);
					}
				}
			}
		}
	}
_abort:
	MPI_Barrier(MPI_COMM_WORLD);
	{
		CHECK_ERROR(j_list_length(batch->list) != 0);
		CHECK_ERROR(!batch_empty);
		//destroy caches on EVERY mpi client
		ret = j_db_schema_delete(schema, batch, ERROR_PARAM);
		CHECK_ERROR(!ret);
		ret = j_batch_execute(batch);
		CHECK_ERROR(!ret);
	}
	MPI_Barrier(MPI_COMM_WORLD);
	j_db_schema_unref(schema);
	if (allow_loop)
	{
		goto _start;
	}
	current_result_step->entry_insert[my_index].operations = n1 * m;
	current_result_step->entry_update[my_index].operations = n1 * m2;
	current_result_step->entry_delete[my_index].operations = n1 * m5;
	current_result_step->iterator_single[my_index].operations = n1 * m3;
	current_result_step->iterator_all[my_index].operations = m4;
	current_result_step->entry_insert[my_index].operations_without_n = m;
	current_result_step->entry_update[my_index].operations_without_n = m2;
	current_result_step->entry_delete[my_index].operations_without_n = m5;
	current_result_step->iterator_single[my_index].operations_without_n = m3;
	current_result_step->iterator_all[my_index].operations_without_n = m4 / n1;
	g_free(names);
	g_free(entry);
	g_free(selector);
	g_free(_error);
}
#endif
