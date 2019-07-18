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
#include <stdio.h>
#include <math.h>
#include <float.h>
#include <glib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <julea.h>
#include <julea-db.h>
#include <julea-internal.h>
#include "afl.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wredundant-decls"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-parameter"
/*these warnings should be ignored because the mockup defines functions which are defined in the included headers of the tested file later*/

#include "mockup-db-internal.h"
//directly including the files under test to allow very fast ad explicit mockups
//this test uses mockups for all backend communication
#include "../lib/db/jdb-schema.c"

#pragma GCC diagnostic pop

//configure here->
#define AFL_NAMESPACE_FORMAT "namespace_%d"
#define AFL_NAME_FORMAT "name_%d"
#define AFL_VARNAME_FORMAT "varname_%d"
#define AFL_LIMIT_SCHEMA 4
#define AFL_LIMIT_SCHEMA_FIELDS 4
#define AFL_LIMIT_SCHEMA_NAMESPACE 4
#define AFL_LIMIT_SCHEMA_NAME 4
#define AFL_LIMIT_STRING_LEN 15
//<-

enum JDBAflEvent
{
	AFL_EVENT_DB_SCHEMA_NEW = 0,
	AFL_EVENT_DB_SCHEMA_REF,
	AFL_EVENT_DB_SCHEMA_ADD_FIELD,
	AFL_EVENT_DB_SCHEMA_GET_FIELD,
	AFL_EVENT_DB_SCHEMA_GET_FIELDS,
	AFL_EVENT_DB_SCHEMA_ADD_INDEX,
	AFL_EVENT_DB_SCHEMA_CREATE_GET,
	AFL_EVENT_DB_SCHEMA_DELETE,
	_AFL_EVENT_EVENT_COUNT,
};
typedef enum JDBAflEvent JDBAflEvent;
struct JDBAflRandomValues
{
	guint schema_index;
	guint namespace;
	guint name;
	guint invalid_switch;
	guint var_name;
	JDBType var_type;
};
typedef struct JDBAflRandomValues JDBAflRandomValues;
//variables->
static JDBSchema* stored_schemas[AFL_LIMIT_SCHEMA];
static JDBType schema_field_types[AFL_LIMIT_SCHEMA][AFL_LIMIT_SCHEMA_FIELDS];
//<-
//allgemein->
static char name_strbuf[AFL_LIMIT_STRING_LEN];
static char namespace_strbuf[AFL_LIMIT_STRING_LEN];
static char varname_strbuf[AFL_LIMIT_STRING_LEN];
static JDBAflRandomValues random_values;
//<-
static void
event_schema_new(void)
{
	GError* error = NULL;
	guint i;
	if (stored_schemas[random_values.schema_index])
		j_db_schema_unref(stored_schemas[random_values.schema_index]);
	stored_schemas[random_values.schema_index] = NULL;
	random_values.namespace = random_values.namespace % AFL_LIMIT_SCHEMA_NAMESPACE;
	random_values.name = random_values.name % AFL_LIMIT_SCHEMA_NAME;
	sprintf(namespace_strbuf, AFL_NAMESPACE_FORMAT, random_values.namespace);
	sprintf(name_strbuf, AFL_NAME_FORMAT, random_values.name);
	switch (random_values.invalid_switch % 3)
	{
	case 2:
		stored_schemas[random_values.schema_index] = j_db_schema_new(namespace_strbuf, NULL, &error);
		J_AFL_DEBUG_ERROR(stored_schemas[random_values.schema_index] != NULL, FALSE, error);
		break;
	case 1:
		stored_schemas[random_values.schema_index] = j_db_schema_new(NULL, name_strbuf, &error);
		J_AFL_DEBUG_ERROR(stored_schemas[random_values.schema_index] != NULL, FALSE, error);
		break;
	case 0:
		stored_schemas[random_values.schema_index] = j_db_schema_new(namespace_strbuf, name_strbuf, &error);
		J_AFL_DEBUG_ERROR(stored_schemas[random_values.schema_index] != NULL, TRUE, error);
		for (i = 0; i < AFL_LIMIT_SCHEMA_FIELDS; i++)
		{
			schema_field_types[random_values.schema_index][i] = _J_DB_TYPE_COUNT;
		}
		break;
	default:
		MYABORT();
	}
}
static void
event_schema_ref(void)
{
	GError* error = NULL;
	JDBSchema* ptr = NULL;
	if (stored_schemas[random_values.schema_index])
	{
		if (stored_schemas[random_values.schema_index]->ref_count != 1)
			MYABORT();
		ptr = j_db_schema_ref(stored_schemas[random_values.schema_index], &error);
		J_AFL_DEBUG_ERROR(ptr != NULL, TRUE, error);
		if (ptr != stored_schemas[random_values.schema_index])
			MYABORT();
		if (stored_schemas[random_values.schema_index]->ref_count != 2)
			MYABORT();
		j_db_schema_unref(stored_schemas[random_values.schema_index]);
		if (stored_schemas[random_values.schema_index]->ref_count != 1)
			MYABORT();
	}
	else
	{
		ptr = j_db_schema_ref(stored_schemas[random_values.schema_index], &error);
		J_AFL_DEBUG_ERROR(ptr != NULL, FALSE, error);
	}
}
static void
event_schema_add_field(void)
{
	gboolean ret;
	GError* error = NULL;
	gboolean ret_expected;
	random_values.var_name = random_values.var_name % AFL_LIMIT_SCHEMA_FIELDS;
	random_values.var_type = random_values.var_type % (_J_DB_TYPE_COUNT + 1);
	sprintf(varname_strbuf, AFL_VARNAME_FORMAT, random_values.var_name);
	ret_expected = stored_schemas[random_values.schema_index] != NULL;
	ret_expected = ret_expected && random_values.var_type < _J_DB_TYPE_COUNT;
	ret_expected = ret_expected && schema_field_types[random_values.schema_index][random_values.var_name] == _J_DB_TYPE_COUNT;
	if (stored_schemas[random_values.schema_index])
		ret_expected = ret_expected && !stored_schemas[random_values.schema_index]->server_side;
	if (random_values.invalid_switch % 2)
	{
		ret_expected = FALSE;
		ret = j_db_schema_add_field(stored_schemas[random_values.schema_index], NULL, random_values.var_type, &error);
	}
	else
		ret = j_db_schema_add_field(stored_schemas[random_values.schema_index], varname_strbuf, random_values.var_type, &error);
	J_AFL_DEBUG_ERROR(ret, ret_expected, error);
	if (ret)
		schema_field_types[random_values.schema_index][random_values.var_name] = random_values.var_type;
}
static void
event_schema_get_field(void)
{
	gboolean ret;
	JDBType type;
	GError* error = NULL;
	gboolean ret_expected;
	random_values.var_name = random_values.var_name % AFL_LIMIT_SCHEMA_FIELDS;
	sprintf(varname_strbuf, AFL_VARNAME_FORMAT, random_values.var_name);
	ret_expected = stored_schemas[random_values.schema_index] != NULL;
	ret_expected = ret_expected && schema_field_types[random_values.schema_index][random_values.var_name] < _J_DB_TYPE_COUNT;
	switch (random_values.invalid_switch % 3)
	{
	case 2:
		ret_expected = FALSE;
		ret = j_db_schema_get_field(stored_schemas[random_values.schema_index], NULL, &type, &error);
		break;
	case 1:
		ret_expected = FALSE;
		ret = j_db_schema_get_field(stored_schemas[random_values.schema_index], varname_strbuf, NULL, &error);
		break;
	case 0:
		ret = j_db_schema_get_field(stored_schemas[random_values.schema_index], varname_strbuf, &type, &error);
		if (ret_expected && ret)
		{
			if (ret != (schema_field_types[random_values.schema_index][random_values.var_name] != _J_DB_TYPE_COUNT))
				MYABORT();
			if (type != schema_field_types[random_values.schema_index][random_values.var_name])
				MYABORT();
		}
		break;
	default:
		MYABORT();
	}
	J_AFL_DEBUG_ERROR(ret, ret_expected, error);
}
static void
event_schema_get_fields(void)
{
	guint i, j, k;
	gboolean found;
	gboolean ret;
	JDBType* types;
	gchar** names;
	JDBType* types_cur;
	gchar** names_cur;
	GError* error = NULL;
	gboolean ret_expected;
	ret_expected = stored_schemas[random_values.schema_index] != NULL;
	switch (random_values.invalid_switch % 3)
	{
	case 2:
		ret_expected = FALSE;
		ret = j_db_schema_get_all_fields(stored_schemas[random_values.schema_index], NULL, &types, &error);
		break;
	case 1:
		ret_expected = FALSE;
		ret = j_db_schema_get_all_fields(stored_schemas[random_values.schema_index], &names, NULL, &error);
		break;
	case 0:
		k = 0;
		for (j = 0; j < AFL_LIMIT_SCHEMA_FIELDS; j++)
			if (schema_field_types[random_values.schema_index][j] != _J_DB_TYPE_COUNT)
				k++;
		ret_expected = ret_expected && k > 0;
		ret = j_db_schema_get_all_fields(stored_schemas[random_values.schema_index], &names, &types, &error);
		if (ret_expected && ret)
		{
			i = 0;
			types_cur = types;
			names_cur = names;
			while (*names_cur)
			{
				found = FALSE;
				for (j = 0; j < AFL_LIMIT_SCHEMA_FIELDS; j++)
				{
					sprintf(varname_strbuf, AFL_VARNAME_FORMAT, j);
					if (!g_strcmp0(varname_strbuf, *names_cur))
					{
						found = TRUE;
						break;
					}
				}
				if (!found)
					MYABORT();
				if (*types_cur != schema_field_types[random_values.schema_index][j])
					MYABORT();
				i++;
				types_cur++;
				names_cur++;
			}
			if (*types_cur != _J_DB_TYPE_COUNT)
				MYABORT();
			if (i != k)
				MYABORT();
			g_free(types);
			g_strfreev(names);
		}
		break;
	default:
		MYABORT();
	}
	J_AFL_DEBUG_ERROR(ret, ret_expected, error);
}
static void
event_schema_add_index(void)
{ //TODO test index function
}
static void
event_schema_create_get(void)
{
	JDBSchema* schema = NULL;
	GError* error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	guint j, k;
	gboolean ret;
	gboolean bool_tmp;
	gboolean ret_expected;
	switch (random_values.invalid_switch % 7)
	{
	case 6:
		mockup_should_fail = TRUE;
		ret = j_db_schema_create(stored_schemas[random_values.schema_index], batch, &error);
		J_AFL_DEBUG_ERROR(ret, FALSE, error);
		break;
	case 5:
		mockup_should_fail = FALSE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		k = 0;
		for (j = 0; j < AFL_LIMIT_SCHEMA_FIELDS; j++)
			if (schema_field_types[random_values.schema_index][j] != _J_DB_TYPE_COUNT)
				k++;
		ret_expected = ret_expected && k > 0;
		if (stored_schemas[random_values.schema_index])
			ret_expected = ret_expected && !stored_schemas[random_values.schema_index]->server_side;
		ret = j_db_schema_create(stored_schemas[random_values.schema_index], batch, &error);
		J_AFL_DEBUG_ERROR(ret, ret_expected, error);
		if (ret_expected && stored_schemas[random_values.schema_index])
		{
			schema = j_db_schema_new(stored_schemas[random_values.schema_index]->namespace, stored_schemas[random_values.schema_index]->name, &error);
			J_AFL_DEBUG_ERROR(schema != NULL, ret_expected, error);
			mockup_should_fail = TRUE;
			ret = j_db_schema_get(schema, batch, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
		}
		break;
	case 4:
		mockup_should_fail = FALSE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		k = 0;
		for (j = 0; j < AFL_LIMIT_SCHEMA_FIELDS; j++)
			if (schema_field_types[random_values.schema_index][j] != _J_DB_TYPE_COUNT)
				k++;
		ret_expected = ret_expected && k > 0;
		if (stored_schemas[random_values.schema_index])
			ret_expected = ret_expected && !stored_schemas[random_values.schema_index]->server_side;
		ret = j_db_schema_create(stored_schemas[random_values.schema_index], batch, &error);
		J_AFL_DEBUG_ERROR(ret, ret_expected, error);
		if (ret_expected && stored_schemas[random_values.schema_index])
		{
			schema = j_db_schema_new(stored_schemas[random_values.schema_index]->namespace, stored_schemas[random_values.schema_index]->name, &error);
			J_AFL_DEBUG_ERROR(schema != NULL, ret_expected, error);
			ret = j_db_schema_get(NULL, batch, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
		}
		break;
	case 3:
		mockup_should_fail = FALSE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		k = 0;
		for (j = 0; j < AFL_LIMIT_SCHEMA_FIELDS; j++)
			if (schema_field_types[random_values.schema_index][j] != _J_DB_TYPE_COUNT)
				k++;
		ret_expected = ret_expected && k > 0;
		if (stored_schemas[random_values.schema_index])
			ret_expected = ret_expected && !stored_schemas[random_values.schema_index]->server_side;
		ret = j_db_schema_create(stored_schemas[random_values.schema_index], batch, &error);
		J_AFL_DEBUG_ERROR(ret, ret_expected, error);
		if (ret_expected && stored_schemas[random_values.schema_index])
		{
			schema = j_db_schema_new(stored_schemas[random_values.schema_index]->namespace, stored_schemas[random_values.schema_index]->name, &error);
			J_AFL_DEBUG_ERROR(schema != NULL, ret_expected, error);
			ret = j_db_schema_get(schema, NULL, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
		}
		break;
	case 2:
		mockup_should_fail = FALSE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		ret = j_db_schema_create(stored_schemas[random_values.schema_index], NULL, &error);
		J_AFL_DEBUG_ERROR(ret, FALSE, error);
		break;
	case 1:
		mockup_should_fail = FALSE;
		ret = j_db_schema_create(NULL, batch, &error);
		J_AFL_DEBUG_ERROR(ret, FALSE, error);
		break;
	case 0:
		mockup_should_fail = FALSE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		k = 0;
		for (j = 0; j < AFL_LIMIT_SCHEMA_FIELDS; j++)
			if (schema_field_types[random_values.schema_index][j] != _J_DB_TYPE_COUNT)
				k++;
		ret_expected = ret_expected && k > 0;
		if (stored_schemas[random_values.schema_index])
			ret_expected = ret_expected && !stored_schemas[random_values.schema_index]->server_side;
		ret = j_db_schema_create(stored_schemas[random_values.schema_index], batch, &error);
		J_AFL_DEBUG_ERROR(ret, ret_expected, error);
		if (ret_expected && stored_schemas[random_values.schema_index])
		{
			schema = j_db_schema_new(stored_schemas[random_values.schema_index]->namespace, stored_schemas[random_values.schema_index]->name, &error);
			J_AFL_DEBUG_ERROR(schema != NULL, ret_expected, error);
			ret = j_db_schema_get(schema, batch, &error);
			J_AFL_DEBUG_ERROR(ret, ret_expected, error);
			ret = j_db_schema_equals(schema, stored_schemas[random_values.schema_index], &bool_tmp, &error);
			J_AFL_DEBUG_ERROR(ret, ret_expected, error);
			if (bool_tmp != ret_expected)
				MYABORT();
		}
		break;
	default:
		MYABORT();
	}
	j_db_schema_unref(schema);
}
static void
event_schema_delete(void)
{
	gboolean ret_expected;
	gboolean ret;
	GError* error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	switch (random_values.invalid_switch % 4)
	{
	case 3:
		mockup_should_fail = TRUE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		ret = j_db_schema_delete(stored_schemas[random_values.schema_index], batch, &error);
		J_AFL_DEBUG_ERROR(ret, FALSE, error);
		break;
	case 2:
		mockup_should_fail = FALSE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		ret = j_db_schema_delete(NULL, batch, &error);
		J_AFL_DEBUG_ERROR(ret, FALSE, error);
		break;
	case 1:
		mockup_should_fail = FALSE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		ret = j_db_schema_delete(stored_schemas[random_values.schema_index], NULL, &error);
		J_AFL_DEBUG_ERROR(ret, FALSE, error);
		break;
	case 0:
		mockup_should_fail = FALSE;
		ret_expected = stored_schemas[random_values.schema_index] != NULL;
		ret = j_db_schema_delete(stored_schemas[random_values.schema_index], batch, &error);
		J_AFL_DEBUG_ERROR(ret, ret_expected, error);
		break;
	default:
		MYABORT();
	}
}

int
main(int argc, char* argv[])
{
	FILE* file;
	JDBAflEvent event;
	guint i;
	if (argc > 1)
	{
		char filename[50 + strlen(argv[1])];
		mkdir(argv[1], S_IRUSR | S_IRGRP | S_IROTH);
		sprintf(filename, "%s/start-files", argv[1]);
		mkdir(filename, S_IRUSR | S_IRGRP | S_IROTH);
		memset(&random_values, 0, sizeof(random_values));
		for (i = 0; i < _AFL_EVENT_EVENT_COUNT; i++)
		{
			sprintf(filename, "%s/start-files/test-db-schema-%d.bin", argv[1], i);
			file = fopen(filename, "wb");
			event = i;
			fwrite(&event, sizeof(event), 1, file);
			fwrite(&random_values, sizeof(random_values), 1, file);
			fclose(file);
		}
		goto fini;
	}
#ifdef __AFL_HAVE_MANUAL_CONTROL
	__AFL_INIT();
	while (__AFL_LOOP(1000))
#endif
	{
	loop:
		MY_READ_MAX(event, _AFL_EVENT_EVENT_COUNT);
		MY_READ(random_values);
		random_values.schema_index = random_values.schema_index % AFL_LIMIT_SCHEMA;
		switch (event)
		{

		case AFL_EVENT_DB_SCHEMA_NEW:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_NEW %d", random_values.schema_index);
			event_schema_new();
			break;
		case AFL_EVENT_DB_SCHEMA_REF:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_REF %d", random_values.schema_index);
			event_schema_ref();
			break;
		case AFL_EVENT_DB_SCHEMA_ADD_FIELD:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_ADD_FIELD %d", random_values.schema_index);
			event_schema_add_field();
			break;
		case AFL_EVENT_DB_SCHEMA_GET_FIELD:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_GET_FIELD %d", random_values.schema_index);
			event_schema_get_field();
			break;
		case AFL_EVENT_DB_SCHEMA_GET_FIELDS:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_GET_FIELDS %d", random_values.schema_index);
			event_schema_get_fields();
			break;
		case AFL_EVENT_DB_SCHEMA_ADD_INDEX:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_ADD_INDEX %d", random_values.schema_index);
			event_schema_add_index();
			break;
		case AFL_EVENT_DB_SCHEMA_CREATE_GET:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_CREATE_GET %d", random_values.schema_index);
			event_schema_create_get();
			break;
		case AFL_EVENT_DB_SCHEMA_DELETE:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_DELETE %d", random_values.schema_index);
			event_schema_delete();
			break;
		case _AFL_EVENT_EVENT_COUNT:
		default:
			MYABORT();
		}
		goto loop;
	cleanup:
		for (i = 0; i < AFL_LIMIT_SCHEMA; i++)
		{
			j_db_schema_unref(stored_schemas[i]);
			stored_schemas[i] = NULL;
		}
	}
fini:
	return 0;
}
