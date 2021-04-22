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
 * Query: SELECT _id, "city", "customerid", "state", "firstname", "lastname", "zip", "address", "email" FROM "namespace_customersTable" 
 * WHERE ( "customerid" = 3 AND "zip" = 22121 OR ( "customerid" = 1 AND "zip" = 22902 ));
 *
 * Expected Output:  
 * No rows
 *
 * Actual Output:
 * 1|Mount Vernon|1|VA|George|Washington|22121|3200 Mt Vernon Hwy|gwashington@usa.gov
 * 3|Charlottesville|3|VA|Thomas|Jefferson|22902|931 Thomas Jefferson Pkwy|tjefferson@usa.gov
 */

static void
try_or_operator_between_two_conditionsWithANDOperator(void)
{
	g_autoptr(GError) error = NULL;

	gboolean success = TRUE;
	g_autoptr(JDBSchema) schema_customersTable = NULL;
	g_autoptr(JDBSelector) selector_customersTable = NULL;
	g_autoptr(JDBSelector) selector2_customersTable = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	guint64 custid_1 = 3;
	guint64 zip_1 = 22121;
	guint64 custid_2 = 1;
	guint64 zip_2 = 22902;
	JDBType type;
	guint64 len;

	schema_customersTable = j_db_schema_new("namespace", "customersTable", &error);
	g_assert_nonnull(schema_customersTable);
	g_assert_no_error(error);
	success = j_db_schema_get(schema_customersTable, batch, &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_batch_execute(batch);
	g_assert_true(success);

	selector_customersTable = j_db_selector_new(schema_customersTable, J_DB_SELECTOR_MODE_OR, &error);
	g_assert_nonnull(selector_customersTable);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector_customersTable, "customerid", J_DB_SELECTOR_OPERATOR_EQ, &custid_1, sizeof(custid_1), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector_customersTable, "zip", J_DB_SELECTOR_OPERATOR_EQ, &zip_1, sizeof(zip_1), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	
	selector2_customersTable = j_db_selector_new(schema_customersTable, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector2_customersTable);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector2_customersTable, "customerid", J_DB_SELECTOR_OPERATOR_EQ, &custid_2, sizeof(custid_2), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	success = j_db_selector_add_field(selector2_customersTable, "zip", J_DB_SELECTOR_OPERATOR_EQ, &zip_2, sizeof(zip_2), &error);
	g_assert_true(success);
	g_assert_no_error(error);
	
	j_db_selector_add_selector(selector_customersTable, selector2_customersTable, NULL);

	iterator = j_db_iterator_new(schema_customersTable, selector_customersTable, &error);
	g_assert_nonnull(iterator);
	g_assert_no_error(error);

	j_db_iterator_next(iterator, NULL);
	{
		g_assert_null(iterator);
		
		/*g_autofree gchar* firstname = NULL;
		g_autofree gchar* lastname = NULL;

		success = j_db_iterator_get_field(iterator, "firstname", &type, (gpointer*)&firstname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);
		success = j_db_iterator_get_field(iterator, "lastname", &type, (gpointer*)&lastname, &len, &error);
		g_assert_true(success);
		g_assert_no_error(error);

		g_assert_cmpstr(firstname, ==, "George");
		g_assert_cmpstr(lastname, ==, "Washington");
		*/
	}
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
test_or_operator_between_two_conditionsWithANDOperator(void)
{
	schema_create_customersTable();
	entry_insert_customersTable(1, "George", "Washington", "gwashington@usa.gov", "3200 Mt Vernon Hwy", "Mount Vernon", "VA", 22121);
	entry_insert_customersTable(2, "John", "Adams", "jadams@usa.gov", "1250 Hancock St", "Quincy", "MA", 12169);
	entry_insert_customersTable(3, "Thomas", "Jefferson", "tjefferson@usa.gov", "931 Thomas Jefferson Pkwy", "Charlottesville", "VA", 22902);
	entry_insert_customersTable(4, "James", "Madison", "jmadison@usa.gov", "11350 Constitution Hwy", "Orange", "VA", 22960);
	entry_insert_customersTable(5, "James", "Monroe", "jmonroe@usa.gov", "2050 James Monroe Pkwy", "Charlottesville", "VA", 22902);

	try_or_operator_between_two_conditionsWithANDOperator();

	//schema_delete_customersTable();
}

void
test_db_operators(void)
{
	g_test_add_func("/db/operators/or_operator_1", test_or_operator_between_two_conditionsWithANDOperator);
}

