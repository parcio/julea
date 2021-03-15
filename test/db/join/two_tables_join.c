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
 * In this test case a JOIN operation is performed on the following two tables. It is taken from http://www.sql-join.com/
 *
 * Table (reffered as ordersTable in this test case):
 * order_id 	order_date 	amount 	customer_id
 * 1 		07/04/1776 	$234.56 	1
 * 2 		03/14/1760 	$78.50 	3
 * 3 		05/23/1784 	$124.00 	2
 * 4 		09/03/1790 	$65.50 	3
 * 5 		07/21/1795 	$25.50 	10
 * 6 		11/27/1787 	$14.40 	9
 *
 *
 * Table (reffered as customersTable in this test case):
 * customer_id 	first_name 	last_name 	email 			address 			city 			state 	zip
 * 1 			George 	Washington 	gwashington@usa.gov 	3200 Mt Vernon Hwy 		Mount Vernon 		VA 	22121
 * 2 			John 		Adams 		jadams@usa.gov 	1250 Hancock St 		Quincy 		MA 	02169
 * 3 			Thomas 	Jefferson 	tjefferson@usa.gov 	931 Thomas Jefferson Pkwy 	Charlottesville 	VA 	22902
 * 4 			James 		Madison 	jmadison@usa.gov 	11350 Constitution Hwy 	Orange 		VA 	22960
 * 5 			James 		Monroe 	jmonroe@usa.gov 	2050 James Monroe Pkwy 	Charlottesville 	VA 	22902
 *
 *
 */

#include <julea-config.h>

#include <glib.h>

#include <julea.h>
#include <julea-db.h>

#include "test.h"

static void
schema_create_ordersTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_orderid[] = {
		"orderid", NULL
	};

	schema = j_db_schema_new("namespace", "ordersTable", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "orderid", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "orderdate", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "amount", J_DB_TYPE_FLOAT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "customerid", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_add_index(schema, idx_orderid, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
entry_insert_ordersTable(guint64 orderid, gchar const* orderdate, gdouble amount, guint64 customerid)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "ordersTable", &error);
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
	success = j_db_entry_set_field(entry, "orderid", &orderid, sizeof(orderid), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "orderdate", orderdate, strlen(orderdate), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "amount", &amount, sizeof(amount), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "customerid", &customerid, sizeof(customerid), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_create_customersTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_customerid[] = {
		"customerid", NULL
	};

	schema = j_db_schema_new("namespace", "customersTable", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "customerid", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "firstname", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "lastname", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "email", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "address", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "city", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "state", J_DB_TYPE_STRING, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_add_field(schema, "zip", J_DB_TYPE_UINT64, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_add_index(schema, idx_customerid, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_db_schema_create(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
entry_insert_customersTable(guint64 customerid, gchar const* firstname, gchar const* lastname, gchar const* email, gchar const* address, gchar const* city, gchar const* state, guint64 zip)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "customersTable", &error);
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
	success = j_db_entry_set_field(entry, "customerid", &customerid, sizeof(customerid), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "firstname", firstname, strlen(firstname), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "lastname", lastname, strlen(lastname), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "email", email, strlen(email), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "address", address, strlen(address), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "city", city, strlen(city), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "state", state, strlen(state), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_set_field(entry, "zip", &zip, sizeof(zip), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_entry_insert(entry, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);
}


static void
schema_delete_ordersTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "ordersTable", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
schema_delete_customersTable(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	schema = j_db_schema_new("namespace", "customersTable", &error);
	g_assert_nonnull(schema);
	g_assert_no_error(error);

	success = j_db_schema_delete(schema, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);

	success = j_batch_execute(batch);
	g_assert_true(success);
}

static void
test_two_tables_join_query_1(void)
{
	schema_create_ordersTable();
	entry_insert_ordersTable(1, "07/04/1776", 234.56, 1);
	entry_insert_ordersTable(2, "03/14/1760", 78.50, 3);
	entry_insert_ordersTable(3, "05/23/1784", 124.00, 2);
	entry_insert_ordersTable(4, "09/03/1790", 65.50, 3);
	entry_insert_ordersTable(5, "07/21/1795", 25.50, 10);
	entry_insert_ordersTable(6, "11/27/1787", 14.40, 9);

	schema_create_customersTable();
	entry_insert_customersTable(1, "George", "Washington", "gwashington@usa.gov", "3200 Mt Vernon Hwy", "Mount Vernon", "VA", 22121);
	entry_insert_customersTable(2, "John", "Adams", "jadams@usa.gov", "1250 Hancock St", "Quincy", "MA", 12169);
	entry_insert_customersTable(3, "Thomas", "Jefferson", "tjefferson@usa.gov", "931 Thomas Jefferson Pkwy", "Charlottesville", "VA", 22902);
	entry_insert_customersTable(4, "James", "Madison", "jmadison@usa.gov", "11350 Constitution Hwy", "Orange", "VA", 22960);
	entry_insert_customersTable(5, "James", "Monroe", "jmonroe@usa.gov", "2050 James Monroe Pkwy", "Charlottesville", "VA", 22902);

	//perform_two_tables_join_without_predicates();
	schema_delete_ordersTable();
	schema_delete_customersTable();
}

static void
test_two_tables_join_query_2(void)
{
	schema_create_ordersTable();
	entry_insert_ordersTable(1, "07/04/1776", 234.56, 1);
	entry_insert_ordersTable(2, "03/14/1760", 78.50, 3);
	entry_insert_ordersTable(3, "05/23/1784", 124.00, 2);
	entry_insert_ordersTable(4, "09/03/1790", 65.50, 3);
	entry_insert_ordersTable(5, "07/21/1795", 25.50, 10);
	entry_insert_ordersTable(6, "11/27/1787", 14.40, 9);

	schema_create_customersTable();
	entry_insert_customersTable(1, "George", "Washington", "gwashington@usa.gov", "3200 Mt Vernon Hwy", "Mount Vernon", "VA", 22121);
	entry_insert_customersTable(2, "John", "Adams", "jadams@usa.gov", "1250 Hancock St", "Quincy", "MA", 12169);
	entry_insert_customersTable(3, "Thomas", "Jefferson", "tjefferson@usa.gov", "931 Thomas Jefferson Pkwy", "Charlottesville", "VA", 22902);
	entry_insert_customersTable(4, "James", "Madison", "jmadison@usa.gov", "11350 Constitution Hwy", "Orange", "VA", 22960);
	entry_insert_customersTable(5, "James", "Monroe", "jmonroe@usa.gov", "2050 James Monroe Pkwy", "Charlottesville", "VA", 22902);

	//perform_two_tables_join_with_predicates();
	schema_delete_ordersTable();
	schema_delete_customersTable();
}

void
test_two_tables_join_queries(void)
{
	// TODO: Both the test cases create and delete tables. More suitable is to add two methods to initialize and delete the shared data (setUp and tearDown).
	g_test_add_func("/db/join/two_tables_join_1", test_two_tables_join_query_1);
	g_test_add_func("/db/join/two_tables_join_2", test_two_tables_join_query_2);
}
