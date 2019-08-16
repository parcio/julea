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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>
#include <julea-kv.h>

#include "test.h"

#define JULEA_TEST_COMPILES

#include "../../test-afl/test-db-backend.c"

static void
test_db_schema_create(void)
{
	memset(&random_values, 0, sizeof(random_values));
	event = AFL_EVENT_DB_SCHEMA_CREATE;
	test_db_backend_exec();
	test_db_backend_cleanup();
}
static void
test_db_schema_get(void)
{
	memset(&random_values, 0, sizeof(random_values));
	event = AFL_EVENT_DB_SCHEMA_GET;
	test_db_backend_exec();
	test_db_backend_cleanup();
}
static void
test_db_schema_delete(void)
{
	memset(&random_values, 0, sizeof(random_values));
	event = AFL_EVENT_DB_SCHEMA_DELETE;
	test_db_backend_exec();
	test_db_backend_cleanup();
}
static void
test_db_insert(void)
{
	memset(&random_values, 0, sizeof(random_values));
	event = AFL_EVENT_DB_INSERT;
	test_db_backend_exec();
	event = AFL_EVENT_DB_SCHEMA_CREATE;
	test_db_backend_exec();
	event = AFL_EVENT_DB_INSERT;
	test_db_backend_exec();
	test_db_backend_cleanup();
}
static void
test_db_update(void)
{
	memset(&random_values, 0, sizeof(random_values));
	event = AFL_EVENT_DB_UPDATE;
	test_db_backend_exec();
	event = AFL_EVENT_DB_SCHEMA_CREATE;
	test_db_backend_exec();
	event = AFL_EVENT_DB_UPDATE;
	test_db_backend_exec();
	event = AFL_EVENT_DB_INSERT;
	test_db_backend_exec();
	event = AFL_EVENT_DB_UPDATE;
	test_db_backend_exec();
	test_db_backend_cleanup();
}
static void
test_db_delete(void)
{
	memset(&random_values, 0, sizeof(random_values));
	event = AFL_EVENT_DB_DELETE;
	test_db_backend_exec();
	event = AFL_EVENT_DB_SCHEMA_CREATE;
	test_db_backend_exec();
	event = AFL_EVENT_DB_INSERT;
	test_db_backend_exec();
	event = AFL_EVENT_DB_DELETE;
	test_db_backend_exec();
	test_db_backend_cleanup();
}
static void
test_db_query_all(void)
{
	memset(&random_values, 0, sizeof(random_values));
	event = AFL_EVENT_DB_QUERY_ALL;
	test_db_backend_exec();
	event = AFL_EVENT_DB_SCHEMA_CREATE;
	test_db_backend_exec();
	event = AFL_EVENT_DB_INSERT;
	test_db_backend_exec();
	event = AFL_EVENT_DB_QUERY_ALL;
	test_db_backend_exec();
	test_db_backend_cleanup();
}

void
test_db_backend(void)
{
	test_db_backend_init();
	g_test_add_func("/db/backend/schema-create", test_db_schema_create);
	g_test_add_func("/db/backend/schema-get", test_db_schema_get);
	g_test_add_func("/db/backend/schema-delete", test_db_schema_delete);
	g_test_add_func("/db/backend/insert", test_db_insert);
	g_test_add_func("/db/backend/update", test_db_update);
	g_test_add_func("/db/backend/delete", test_db_delete);
	g_test_add_func("/db/backend/query_all", test_db_query_all);
}
