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

#include <julea-config.h>

#include <glib.h>
#include <math.h>

#include <julea.h>
#include <julea-db.h>

#include "test.h"

static JDBSchema*
generate_test_schema(gchar const* name)
{
	g_autoptr(JDBSchema) schema = NULL;
	gboolean res;
	gchar field_name[] = "field_dd";

	schema = j_db_schema_new("test-ns", name, NULL);

	for (int i = 0; i <= 99; ++i)
	{
		g_snprintf(field_name, sizeof(field_name), "field_%i", i);
		res = j_db_schema_add_field(schema, field_name, i % 8, NULL);
		g_assert_true(res);
	}

	return j_db_schema_ref(schema);
}

static void
test_db_selector_add_field(void)
{
	J_TEST_TRAP_START
	g_autoptr(JDBSchema) schema = generate_test_schema("selector-test");
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(GError) error = NULL;
	gboolean ret;
	gchar val[] = "some value";
	gchar field_name[] = "field_dd";

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	g_assert_no_error(error);

	for (int i = 0; i < 8; ++i)
	{
		g_snprintf(field_name, sizeof(field_name), "field_%i", i);
		ret = j_db_selector_add_field(selector, field_name, i % 6, val, sizeof(val), &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		if (g_test_failed())
			return;
	}
	J_TEST_TRAP_END
}

static void
test_db_selector_add_non_existant_field(void)
{
	J_TEST_TRAP_START
	g_autoptr(JDBSchema) schema = generate_test_schema("selector-test");
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(GError) error = NULL;
	gboolean ret;
	gchar val[] = "some value";

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector);
	g_assert_no_error(error);

	ret = j_db_selector_add_field(selector, "field_100", J_DB_SELECTOR_OPERATOR_GT, val, sizeof(val), &error);
	g_assert_false(ret);
	g_assert_nonnull(error);
	J_TEST_TRAP_END
}

static void
test_db_selector_add_selector(void)
{
	J_TEST_TRAP_START
	g_autoptr(JDBSchema) schema = generate_test_schema("selector-test");
	g_autoptr(JDBSelector) selector1 = NULL;
	g_autoptr(JDBSelector) selector2 = NULL;
	g_autoptr(GError) error = NULL;
	gboolean ret;
	gchar val[] = "some value";
	gchar field_name[] = "field_dd";

	selector1 = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, &error);
	g_assert_nonnull(selector1);
	g_assert_no_error(error);

	selector2 = j_db_selector_new(schema, J_DB_SELECTOR_MODE_OR, &error);
	g_assert_nonnull(selector2);
	g_assert_no_error(error);

	for (int i = 0; i < 8; ++i)
	{
		g_snprintf(field_name, sizeof(field_name), "field_%i", i);
		ret = j_db_selector_add_field(selector1, field_name, i % 6, val, sizeof(val), &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		if (g_test_failed())
			return;
	}

	for (int i = 8; i < 25; ++i)
	{
		g_snprintf(field_name, sizeof(field_name), "field_%i", i);
		ret = j_db_selector_add_field(selector2, field_name, i % 6, val, sizeof(val), &error);
		g_assert_true(ret);
		g_assert_no_error(error);
		if (g_test_failed())
			return;
	}

	ret = j_db_selector_add_selector(selector1, selector2, &error);
	g_assert_true(ret);
	g_assert_no_error(error);
	J_TEST_TRAP_END
}

void
test_db_selector(void)
{
	g_test_add_func("/db/selector/add-field", test_db_selector_add_field);
	g_test_add_func("/db/selector/add-non-existant-field", test_db_selector_add_non_existant_field);
	g_test_add_func("/db/selector/add-selector", test_db_selector_add_selector);
}
