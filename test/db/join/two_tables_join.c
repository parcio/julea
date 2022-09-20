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

/*
 * Query: SELECT namespace_customersTable._id, namespace_customersTable."city", namespace_customersTable."customerid", namespace_customersTable."state", 
 * namespace_customersTable."firstname", namespace_customersTable."lastname", namespace_customersTable."zip", namespace_customersTable."address", 
 * namespace_customersTable."email", namespace_ordersTable._id, namespace_ordersTable."orderid", namespace_ordersTable."amount", namespace_ordersTable."orderdate", 
 * namespace_ordersTable."customerid" FROM "namespace_customersTable", "namespace_ordersTable" WHERE namespace_customersTable.customerid = namespace_ordersTable.customerid 
 * AND  ( namespace_customersTable."customerid"= 3 );
 *
 * Output: 
 * ... order_id 	order_date 	order_amount	...
 * ... 2 		3/14/1760 	$78.50		...
 * ... 4 		9/03/1790 	$65.50		...
 */

static void
perform_two_tables_join_with_predicates(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema_ordersTable = NULL;
	g_autoptr(JDBSchema) schema_customersTable = NULL;
	g_autoptr(JDBSelector) selector_ordersTable = NULL;
	g_autoptr(JDBSelector) selector_customersTable = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	guint64 val = 3;
	JDBType type;
	guint64 len;

	schema_ordersTable = j_db_schema_new("namespace", "ordersTable", &error);
	g_assert_nonnull(schema_ordersTable);
	g_assert_no_error(error);
	schema_customersTable = j_db_schema_new("namespace", "customersTable", &error);
	g_assert_nonnull(schema_customersTable);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_ordersTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_customersTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector_ordersTable = j_db_selector_new(schema_ordersTable, J_DB_SELECTOR_MODE_OR, &error);
	g_assert_nonnull(selector_ordersTable);
	g_assert_no_error(error);
	selector_customersTable = j_db_selector_new(schema_customersTable, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector_customersTable);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector_customersTable, "customerid", J_DB_SELECTOR_OPERATOR_EQ, &val, sizeof(val), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	j_db_selector_add_selector(selector_customersTable, selector_ordersTable, NULL);
	j_db_selector_add_join(selector_customersTable, "customerid", selector_ordersTable, "customerid", NULL);

	iterator = j_db_iterator_new(schema_customersTable, selector_customersTable, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* orderdate = NULL;
		g_autofree guint64* orderid = NULL;
		g_autofree gchar* firstname = NULL;
		g_autofree gchar* lastname = NULL;
		g_autofree gdouble* amount = NULL;
		g_autofree guint64* customerid = NULL;

		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderid", &type, (gpointer*)&orderid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderdate", &type, (gpointer*)&orderdate, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "amount", &type, (gpointer*)&amount, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "customerid", &type, (gpointer*)&customerid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "firstname", &type, (gpointer*)&firstname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "lastname", &type, (gpointer*)&lastname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(firstname, ==, "Thomas");
		g_assert_cmpstr(lastname, ==, "Jefferson");
		g_assert_true(*orderid == (guint64)2);
		g_assert_true(*customerid == (guint64)3);
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* orderdate = NULL;
		g_autofree guint64* orderid = NULL;
		g_autofree gchar* firstname = NULL;
		g_autofree gchar* lastname = NULL;
		g_autofree gdouble* amount = NULL;
		g_autofree guint64* customerid = NULL;

		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderid", &type, (gpointer*)&orderid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderdate", &type, (gpointer*)&orderdate, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "amount", &type, (gpointer*)&amount, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "customerid", &type, (gpointer*)&customerid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "firstname", &type, (gpointer*)&firstname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "lastname", &type, (gpointer*)&lastname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(firstname, ==, "Thomas");
		g_assert_cmpstr(lastname, ==, "Jefferson");
		g_assert_true(*orderid == (guint64)4);
		g_assert_true(*customerid == (guint64)3);
	}
}

/*
 * Query: SELECT namespace_customersTable._id, namespace_customersTable."city", namespace_customersTable."customerid", namespace_customersTable."state", 
 * namespace_customersTable."firstname", namespace_customersTable."lastname", namespace_customersTable."zip", namespace_customersTable."address", 
 * namespace_customersTable."email", namespace_ordersTable._id, namespace_ordersTable."orderid", namespace_ordersTable."amount", namespace_ordersTable."orderdate", 
 * namespace_ordersTable."customerid" FROM "namespace_customersTable", "namespace_ordersTable" WHERE namespace_customersTable.customerid = namespace_ordersTable.customerid; 
 *
 * Output: 
 * ... order_id 	order_date 	order_amount	...
 * ... 1 		7/04/1776 	$234.56	...
 * ... 3 		3/14/1760 	$780.50	...
 * ... 2 		3/14/1760 	$78.50		...
 * ... 3 		9/03/1790 	$65.50		...
 */

static void
perform_two_tables_join_without_predicates(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema_ordersTable = NULL;
	g_autoptr(JDBSchema) schema_customersTable = NULL;
	g_autoptr(JDBSelector) selector_ordersTable = NULL;
	g_autoptr(JDBSelector) selector_customersTable = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	JDBType type;
	guint64 len;

	schema_ordersTable = j_db_schema_new("namespace", "ordersTable", &error);
	g_assert_nonnull(schema_ordersTable);
	g_assert_no_error(error);
	schema_customersTable = j_db_schema_new("namespace", "customersTable", &error);
	g_assert_nonnull(schema_customersTable);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_ordersTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_customersTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector_ordersTable = j_db_selector_new(schema_ordersTable, J_DB_SELECTOR_MODE_OR, &error);
	g_assert_nonnull(selector_ordersTable);
	g_assert_no_error(error);
	selector_customersTable = j_db_selector_new(schema_customersTable, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector_customersTable);
	g_assert_no_error(error);
	j_db_selector_add_selector(selector_customersTable, selector_ordersTable, NULL);
	j_db_selector_add_join(selector_customersTable, "customerid", selector_ordersTable, "customerid", NULL);

	iterator = j_db_iterator_new(schema_customersTable, selector_customersTable, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* orderdate = NULL;
		g_autofree guint64* orderid = NULL;
		g_autofree gchar* firstname = NULL;
		g_autofree gchar* lastname = NULL;
		g_autofree gdouble* amount = NULL;
		g_autofree guint64* customerid = NULL;

		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderid", &type, (gpointer*)&orderid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderdate", &type, (gpointer*)&orderdate, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "amount", &type, (gpointer*)&amount, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "customerid", &type, (gpointer*)&customerid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "firstname", &type, (gpointer*)&firstname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "lastname", &type, (gpointer*)&lastname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(firstname, ==, "George");
		g_assert_cmpstr(lastname, ==, "Washington");
		g_assert_true(*orderid == (guint64)1);
		g_assert_true(*customerid == (guint64)1);
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* orderdate = NULL;
		g_autofree guint64* orderid = NULL;
		g_autofree gchar* firstname = NULL;
		g_autofree gchar* lastname = NULL;
		g_autofree gdouble* amount = NULL;
		g_autofree guint64* customerid = NULL;

		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderid", &type, (gpointer*)&orderid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderdate", &type, (gpointer*)&orderdate, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "amount", &type, (gpointer*)&amount, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "customerid", &type, (gpointer*)&customerid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "firstname", &type, (gpointer*)&firstname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "lastname", &type, (gpointer*)&lastname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		/* MySql and Sqlite returns the results in different order, therefore ignoring the following lines until a way is deduced to sort the result items first.
		g_assert_cmpstr(firstname, ==, "John");
		g_assert_cmpstr(lastname, ==, "Adams");
		g_assert_true(*orderid == (guint64)3);
		g_assert_true(*customerid == (guint64)2);
		*/
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* orderdate = NULL;
		g_autofree guint64* orderid = NULL;
		g_autofree gchar* firstname = NULL;
		g_autofree gchar* lastname = NULL;
		g_autofree gdouble* amount = NULL;
		g_autofree guint64* customerid = NULL;

		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderid", &type, (gpointer*)&orderid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderdate", &type, (gpointer*)&orderdate, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "amount", &type, (gpointer*)&amount, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "customerid", &type, (gpointer*)&customerid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "firstname", &type, (gpointer*)&firstname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "lastname", &type, (gpointer*)&lastname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		/* MySql and Sqlite returns the results in different order, therefore ignoring the following lines until a way is deduced to sort the result items first.
		g_assert_cmpstr(firstname, ==, "Thomas");
		g_assert_cmpstr(lastname, ==, "Jefferson");
		g_assert_true(*orderid == (guint64)2);
		g_assert_true(*customerid == (guint64)3);
		*/
	}

	j_db_iterator_next(iterator, NULL);
	{
		g_autofree gchar* orderdate = NULL;
		g_autofree guint64* orderid = NULL;
		g_autofree gchar* firstname = NULL;
		g_autofree gchar* lastname = NULL;
		g_autofree gdouble* amount = NULL;
		g_autofree guint64* customerid = NULL;

		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderid", &type, (gpointer*)&orderid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "orderdate", &type, (gpointer*)&orderdate, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "amount", &type, (gpointer*)&amount, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "ordersTable", "customerid", &type, (gpointer*)&customerid, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "firstname", &type, (gpointer*)&firstname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field_ex(iterator, "customersTable", "lastname", &type, (gpointer*)&lastname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(firstname, ==, "Thomas");
		g_assert_cmpstr(lastname, ==, "Jefferson");
		g_assert_true(*orderid == (guint64)4);
		g_assert_true(*customerid == (guint64)3);
	}
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

	perform_two_tables_join_without_predicates();
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

	perform_two_tables_join_with_predicates();
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
