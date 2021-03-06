/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 * Copyright (C) 2019-2021 Michael Kuhn
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


/*
 * In this test case a JOIN operation is performed on the following three tables. 
 * It is taken from https://javarevisited.blogspot.com/2012/11/how-to-join-three-tables-in-sql-query-mysql-sqlserver.html#axzz6oIxnmRfc.
 *
 * Table (reffered as TableA in this test case):
 * +--------+----------+--------+
 *| emp_id | emp_name | salary |
 *+--------+----------+--------+
 *| 1      | James    |   2000 |
 *| 2      | Jack     |   4000 |
 *| 3      | Henry    |   6000 |
 *| 4      | Tom      |   8000
 *
 *
 * Table (reffered as TableB in this test case):
 *+---------+-----------+
 *| dept_id | dept_name |
 *+---------+-----------+
 *| 101     | Sales     |
 *| 102     | Marketing |
 *| 103     | Finance   |
 *+---------+-----------+
 *
 *
 * Table (reffered as TableC in this test case):
 *+--------+---------+
 *| emp_id | dept_id |
 *+--------+---------+
 *|      1 |     101 |
 *|      2 |     102 |
 *|      3 |     103 |
 *|      4 |     102 |
 *+--------+---------+
 *
 * Query: SELECT namespace_tableC._id, namespace_tableC."deptid", namespace_tableC."empid", namespace_tableA._id, namespace_tableA."empname", namespace_tableA."salary", 
 * namespace_tableA."empid", namespace_tableB._id, namespace_tableB."deptid", namespace_tableB."deptname" FROM "namespace_tableC", "namespace_tableA", "namespace_tableB" 
 * WHERE namespace_tableC.empid=namespace_tableA.empid AND namespace_tableC.deptid=namespace_tableB.deptid
 *
 * Output: 
 *+----------+-----------+
 *| emp_name | dept_name |
 *+----------+-----------+
 *| James    | Sales     |
 *| Jack     | Marketing |
 *| Henry    | Finance   |
 *| Tom      | Marketing |
 *+----------+-----------+
 *
 */


#include <julea-config.h>

#include <glib.h>

#include <julea.h>
#include <julea-db.h>

#include "test.h"

static void
schema_create_tableA(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_empid[] = {
		"empid", NULL
	};

	schema = j_db_schema_new("namespace", "tableA", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "empid", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "empname", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "salary", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_add_index(schema, idx_empid, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
entry_insert_tableA(guint64 empid, gchar const* empname, guint64 salary)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "tableA", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "empid", &empid, sizeof(empid), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "empname", empname, strlen(empname), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "salary", &salary, sizeof(salary), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_create_tableB(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_deptid[] = {
		"deptid", NULL
	};

	schema = j_db_schema_new("namespace", "tableB", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "deptid", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "deptname", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_add_index(schema, idx_deptid, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
entry_insert_tableB(guint64 deptid, gchar const* deptname)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "tableB", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "deptid", &deptid, sizeof(deptid), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "deptname", deptname, strlen(deptname), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_create_tableC(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_empid[] = {
		"empid", NULL
	};

	schema = j_db_schema_new("namespace", "tableC", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "empid", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "deptid", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_add_index(schema, idx_empid, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
entry_insert_tableC(guint64 empid, guint64 deptid)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "tableC", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_get(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	entry = j_db_entry_new(schema, &error);
	g_assert_nonnull(entry);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "empid", &empid, sizeof(empid), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "deptid", &deptid, sizeof(deptid), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
perform_join_on_table1_table2_table3(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema_tableA = NULL;
	g_autoptr(JDBSchema) schema_tableB = NULL;
	g_autoptr(JDBSchema) schema_tableC = NULL;
	g_autoptr(JDBSelector) selector_tableA = NULL;
	g_autoptr(JDBSelector) selector_tableB = NULL;
	g_autoptr(JDBSelector) selector_tableC = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	JDBType type;
	guint64 len;

	schema_tableA = j_db_schema_new("namespace", "tableA", &error);
	g_assert_nonnull(schema_tableA);
	g_assert_no_error(error);
	schema_tableB = j_db_schema_new("namespace", "tableB", &error);
	g_assert_nonnull(schema_tableB);
	g_assert_no_error(error);
	schema_tableC = j_db_schema_new("namespace", "tableC", &error);
	g_assert_nonnull(schema_tableC);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_tableA, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_tableB, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_tableC, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector_tableA = j_db_selector_new(schema_tableA, J_DB_SELECTOR_MODE_OR, &error);
	g_assert_nonnull(selector_tableA);
	g_assert_no_error(error);
	selector_tableB = j_db_selector_new(schema_tableB, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector_tableB);
	g_assert_no_error(error);
	selector_tableC = j_db_selector_new(schema_tableC, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector_tableC);
	g_assert_no_error(error);
	j_db_selector_add_selector(selector_tableC, selector_tableA, NULL);
	j_db_selector_add_selector(selector_tableC, selector_tableB, NULL);
	j_db_selector_add_join(selector_tableC, "empid", selector_tableA, "empid", NULL);
	j_db_selector_add_join(selector_tableC, "deptid", selector_tableB, "deptid", NULL);

	iterator = j_db_iterator_new(schema_tableC, selector_tableC, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "namespace", "tableA","empname", &type, &empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "namespace", "tableB","deptname", &type, &deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "James");
		g_assert_cmpstr(deptname, ==, "Sales");
	}


	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "namespace", "tableA","empname", &type, &empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "namespace", "tableB","deptname", &type, &deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "Jack");
		g_assert_cmpstr(deptname, ==, "Marketing");
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "namespace", "tableA","empname", &type, &empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "namespace", "tableB","deptname", &type, &deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "Henry");
		g_assert_cmpstr(deptname, ==, "Finance");
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "namespace", "tableA","empname", &type, &empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "namespace", "tableB","deptname", &type, &deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "Tom");
		g_assert_cmpstr(deptname, ==, "Marketing");
	}
}

static void
schema_delete_tableA(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "tableA", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_delete_tableB(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "tableB", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_delete_tableC(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "tableC", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
test_three_tables_join_query_1(void)
{
	schema_create_tableA();
	entry_insert_tableA(1,"James", 2000);
	entry_insert_tableA(2,"Jack", 4000);
	entry_insert_tableA(3,"Henry", 6000);
	entry_insert_tableA(4,"Tom", 8000);

	schema_create_tableB();
	entry_insert_tableB(101,"Sales");
	entry_insert_tableB(102,"Marketing");
	entry_insert_tableB(103,"Finance");
	
	schema_create_tableC();
	entry_insert_tableC(1, 101);
	entry_insert_tableC(2, 102);
	entry_insert_tableC(3, 103);
	entry_insert_tableC(4, 102);

	perform_join_on_table1_table2_table3();
	schema_delete_tableA();
	schema_delete_tableB();
	schema_delete_tableC();
}

void
test_three_tables_join_queries(void)
{
	// FIXME add more tests
	g_test_add_func("/db/join/three_tables_join", test_three_tables_join_query_1);
}
