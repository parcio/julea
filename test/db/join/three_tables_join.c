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
 * Table (reffered as empTable in this test case):
 * +--------+----------+--------+
 *| emp_id | emp_name | salary |
 *+--------+----------+--------+
 *| 1      | James    |   2000 |
 *| 2      | Jack     |   4000 |
 *| 3      | Henry    |   6000 |
 *| 4      | Tom      |   8000
 *
 *
 * Table (reffered as deptTable in this test case):
 *+---------+-----------+
 *| dept_id | dept_name |
 *+---------+-----------+
 *| 101     | Sales     |
 *| 102     | Marketing |
 *| 103     | Finance   |
 *+---------+-----------+
 *
 *
 * Table (reffered as refTable in this test case):
 *+--------+---------+
 *| emp_id | dept_id |
 *+--------+---------+
 *|      1 |     101 |
 *|      2 |     102 |
 *|      3 |     103 |
 *|      4 |     102 |
 *+--------+---------+
 *
 */

#include <julea-config.h>

#include <glib.h>

#include <julea.h>
#include <julea-db.h>

#include "test.h"

static void
schema_create_empTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_empid[] = {
		"empid", NULL
	};

	schema = j_db_schema_new("namespace", "empTable", &error);
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
entry_insert_empTable(guint64 empid, gchar const* empname, guint64 salary)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "empTable", &error);
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
schema_create_deptTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_deptid[] = {
		"deptid", NULL
	};

	schema = j_db_schema_new("namespace", "deptTable", &error);
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
entry_insert_deptTable(guint64 deptid, gchar const* deptname)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "deptTable", &error);
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
schema_create_refTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_empid[] = {
		"empid", NULL
	};

	schema = j_db_schema_new("namespace", "refTable", &error);
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
entry_insert_refTable(guint64 empid, guint64 deptid)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "refTable", &error);
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

/*
 * Query: SELECT namespace_refTable._id, namespace_refTable."deptid", namespace_refTable."empid", namespace_empTable._id, namespace_empTable."empname", namespace_empTable."salary", 
 * namespace_empTable."empid", namespace_deptTable._id, namespace_deptTable."deptid", namespace_deptTable."deptname" FROM "namespace_refTable", "namespace_empTable", "namespace_deptTable" 
 * WHERE namespace_refTable.empid=namespace_empTable.empid AND namespace_refTable.deptid=namespace_deptTable.deptid
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
static void
perform_three_tables_join_without_predicates(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema_empTable = NULL;
	g_autoptr(JDBSchema) schema_deptTable = NULL;
	g_autoptr(JDBSchema) schema_refTable = NULL;
	g_autoptr(JDBSelector) selector_empTable = NULL;
	g_autoptr(JDBSelector) selector_deptTable = NULL;
	g_autoptr(JDBSelector) selector_refTable = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	JDBType type;
	guint64 len;

	schema_empTable = j_db_schema_new("namespace", "empTable", &error);
	g_assert_nonnull(schema_empTable);
	g_assert_no_error(error);
	schema_deptTable = j_db_schema_new("namespace", "deptTable", &error);
	g_assert_nonnull(schema_deptTable);
	g_assert_no_error(error);
	schema_refTable = j_db_schema_new("namespace", "refTable", &error);
	g_assert_nonnull(schema_refTable);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_empTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_deptTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_refTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector_empTable = j_db_selector_new(schema_empTable, J_DB_SELECTOR_MODE_OR, &error);
	g_assert_nonnull(selector_empTable);
	g_assert_no_error(error);
	selector_deptTable = j_db_selector_new(schema_deptTable, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector_deptTable);
	g_assert_no_error(error);
	selector_refTable = j_db_selector_new(schema_refTable, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector_refTable);
	g_assert_no_error(error);
	j_db_selector_add_selector(selector_refTable, selector_empTable, NULL);
	j_db_selector_add_selector(selector_refTable, selector_deptTable, NULL);
	j_db_selector_add_join(selector_refTable, "empid", selector_empTable, "empid", NULL);
	j_db_selector_add_join(selector_refTable, "deptid", selector_deptTable, "deptid", NULL);

	iterator = j_db_iterator_new(schema_refTable, selector_refTable, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "empTable", "empname", &type, (gpointer*)&empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "deptTable", "deptname", &type, (gpointer*)&deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "James");
		g_assert_cmpstr(deptname, ==, "Sales");
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "empTable", "empname", &type, (gpointer*)&empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "deptTable", "deptname", &type, (gpointer*)&deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "Jack");
		g_assert_cmpstr(deptname, ==, "Marketing");
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "empTable", "empname", &type, (gpointer*)&empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "deptTable", "deptname", &type, (gpointer*)&deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "Henry");
		g_assert_cmpstr(deptname, ==, "Finance");
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "empTable", "empname", &type, (gpointer*)&empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "deptTable", "deptname", &type, (gpointer*)&deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "Tom");
		g_assert_cmpstr(deptname, ==, "Marketing");
	}
}

/*
 * Query: SELECT namespace_refTable._id, namespace_refTable."deptid", namespace_refTable."empid", namespace_empTable._id, namespace_empTable."empname", namespace_empTable."salary", 
 * namespace_empTable."empid", namespace_deptTable._id, namespace_deptTable."deptid", namespace_deptTable."deptname" FROM "namespace_refTable", "namespace_empTable", "namespace_deptTable" 
 * WHERE namespace_refTable.empid=namespace_empTable.empid AND namespace_refTable.deptid=namespace_deptTable.deptid
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
static void
perform_three_tables_join_with_predicates(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema_empTable = NULL;
	g_autoptr(JDBSchema) schema_deptTable = NULL;
	g_autoptr(JDBSchema) schema_refTable = NULL;
	g_autoptr(JDBSelector) selector_empTable = NULL;
	g_autoptr(JDBSelector) selector_deptTable = NULL;
	g_autoptr(JDBSelector) selector_refTable = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	guint64 val = 3;
	JDBType type;
	guint64 len;

	schema_empTable = j_db_schema_new("namespace", "empTable", &error);
	g_assert_nonnull(schema_empTable);
	g_assert_no_error(error);
	schema_deptTable = j_db_schema_new("namespace", "deptTable", &error);
	g_assert_nonnull(schema_deptTable);
	g_assert_no_error(error);
	schema_refTable = j_db_schema_new("namespace", "refTable", &error);
	g_assert_nonnull(schema_refTable);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_empTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_deptTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_refTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector_empTable = j_db_selector_new(schema_empTable, J_DB_SELECTOR_MODE_OR, &error);
	g_assert_nonnull(selector_empTable);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector_empTable, "empid", J_DB_SELECTOR_OPERATOR_EQ, &val, sizeof(val), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	selector_deptTable = j_db_selector_new(schema_deptTable, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector_deptTable);
	g_assert_no_error(error);
	selector_refTable = j_db_selector_new(schema_refTable, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector_refTable);
	g_assert_no_error(error);
	j_db_selector_add_selector(selector_refTable, selector_empTable, NULL);
	j_db_selector_add_selector(selector_refTable, selector_deptTable, NULL);
	j_db_selector_add_join(selector_refTable, "empid", selector_empTable, "empid", NULL);
	j_db_selector_add_join(selector_refTable, "deptid", selector_deptTable, "deptid", NULL);

	iterator = j_db_iterator_new(schema_refTable, selector_refTable, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* empname = NULL;
		g_autofree gchar* deptname = NULL;

		success = j_db_iterator_get_field_ex(iterator, "empTable", "empname", &type, (gpointer*)&empname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "deptTable", "deptname", &type, (gpointer*)&deptname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(empname, ==, "Henry");
		g_assert_cmpstr(deptname, ==, "Finance");
	}
}

static void
schema_delete_empTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "empTable", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_delete_deptTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "deptTable", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_delete_refTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "refTable", &error);
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
	schema_create_empTable();
	entry_insert_empTable(1, "James", 2000);
	entry_insert_empTable(2, "Jack", 4000);
	entry_insert_empTable(3, "Henry", 6000);
	entry_insert_empTable(4, "Tom", 8000);

	schema_create_deptTable();
	entry_insert_deptTable(101, "Sales");
	entry_insert_deptTable(102, "Marketing");
	entry_insert_deptTable(103, "Finance");

	schema_create_refTable();
	entry_insert_refTable(1, 101);
	entry_insert_refTable(2, 102);
	entry_insert_refTable(3, 103);
	entry_insert_refTable(4, 102);

	perform_three_tables_join_without_predicates();
	schema_delete_empTable();
	schema_delete_deptTable();
	schema_delete_refTable();
}

static void
test_three_tables_join_query_2(void)
{
	schema_create_empTable();
	entry_insert_empTable(1, "James", 2000);
	entry_insert_empTable(2, "Jack", 4000);
	entry_insert_empTable(3, "Henry", 6000);
	entry_insert_empTable(4, "Tom", 8000);

	schema_create_deptTable();
	entry_insert_deptTable(101, "Sales");
	entry_insert_deptTable(102, "Marketing");
	entry_insert_deptTable(103, "Finance");

	schema_create_refTable();
	entry_insert_refTable(1, 101);
	entry_insert_refTable(2, 102);
	entry_insert_refTable(3, 103);
	entry_insert_refTable(4, 102);

	perform_three_tables_join_with_predicates();
	schema_delete_empTable();
	schema_delete_deptTable();
	schema_delete_refTable();
}

void
test_three_tables_join_queries(void)
{
	// TODO: Both the test cases create and delete tables. More suitable is to add two methods to initialize and delete the shared data (setUp and tearDown).
	g_test_add_func("/db/join/three_tables_join_1", test_three_tables_join_query_1);
	g_test_add_func("/db/join/three_tables_join_2", test_three_tables_join_query_2);
}
