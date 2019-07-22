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
#include <db/jdb-internal.h>
#include <julea-internal.h>
#include <julea-db.h>
#include "afl.h"

//configure here->
#define AFL_NAMESPACE_FORMAT "namespace_%d"
#define AFL_NAME_FORMAT "name_%d"
#define AFL_VARNAME_FORMAT "varname_%d"
#define AFL_STRING_CONST_FORMAT "value_%d"
#define AFL_LIMIT_STRING_LEN 15
#define AFL_LIMIT_SCHEMA_NAMESPACE 4
#define AFL_LIMIT_SCHEMA_NAME 4
#define AFL_LIMIT_SCHEMA_VARIABLES 4
#define AFL_LIMIT_SCHEMA_VALUES 4
#define AFL_LIMIT_SCHEMA_STRING_VALUES (AFL_LIMIT_SCHEMA_VALUES + 0)
//<-

enum JDBAflEvent
{
	AFL_EVENT_DB_SCHEMA_CREATE = 0,
	AFL_EVENT_DB_SCHEMA_GET,
	AFL_EVENT_DB_SCHEMA_DELETE,
	AFL_EVENT_DB_INSERT,
	AFL_EVENT_DB_UPDATE,
	AFL_EVENT_DB_DELETE,
	AFL_EVENT_DB_QUERY_ALL,
	_AFL_EVENT_DB_COUNT,
};
typedef enum JDBAflEvent JDBAflEvent;

struct JDBAflRandomValues
{
	guint namespace;
	guint name;
	union
	{
		struct
		{
			guint variable_count; //variables to create in schema
			JDBType variable_types[AFL_LIMIT_SCHEMA_VARIABLES]; //the given types in this schema
			guint duplicate_variables; //mod 2 -> Yes/No
			guint invalid_bson_schema;
		} schema_create;
		struct
		{
			union
			{
				gdouble d; //random double
				guint64 i; //random int
				guint s; //random string INDEX (point to namespace_varvalues_string_const)
			} values[AFL_LIMIT_SCHEMA_VARIABLES][2];
			guint value_count; //insert <real number of vaiables in schema> + ((mod 3) - 1)
			guint value_index; //"primary key" first column
			guint existent; //mod 2 -> Yes/No
			guint invalid_bson_selector;
			guint invalid_bson_metadata;
		} values;
	};
};
typedef struct JDBAflRandomValues JDBAflRandomValues;
/*TODO test multiple functions/batch */
//schema->
static gboolean namespace_exist[AFL_LIMIT_SCHEMA_NAMESPACE][AFL_LIMIT_SCHEMA_NAME];
static bson_t* namespace_bson[AFL_LIMIT_SCHEMA_NAMESPACE][AFL_LIMIT_SCHEMA_NAME];
static guint namespace_varcount[AFL_LIMIT_SCHEMA_NAMESPACE][AFL_LIMIT_SCHEMA_NAME];
static JDBType namespace_vartypes[AFL_LIMIT_SCHEMA_NAMESPACE][AFL_LIMIT_SCHEMA_NAME][AFL_LIMIT_SCHEMA_VARIABLES];
//<-
//values->
static char namespace_varvalues_string_const[AFL_LIMIT_SCHEMA_STRING_VALUES][AFL_LIMIT_STRING_LEN];
//
static guint64 namespace_varvalues_int64[AFL_LIMIT_SCHEMA_NAMESPACE][AFL_LIMIT_SCHEMA_NAME][AFL_LIMIT_SCHEMA_VALUES][AFL_LIMIT_SCHEMA_VARIABLES];
static gdouble namespace_varvalues_double[AFL_LIMIT_SCHEMA_NAMESPACE][AFL_LIMIT_SCHEMA_NAME][AFL_LIMIT_SCHEMA_VALUES][AFL_LIMIT_SCHEMA_VARIABLES];
static guint namespace_varvalues_string[AFL_LIMIT_SCHEMA_NAMESPACE][AFL_LIMIT_SCHEMA_NAME][AFL_LIMIT_SCHEMA_VALUES][AFL_LIMIT_SCHEMA_VARIABLES];
static guint namespace_varvalues_valid[AFL_LIMIT_SCHEMA_NAMESPACE][AFL_LIMIT_SCHEMA_NAME][AFL_LIMIT_SCHEMA_VALUES]; // (x == 0) -> row does not exist, (x > 0) -> row exist with given number of valid columns
//<-
//allgemein->
static JDBAflRandomValues random_values;
static char namespace_strbuf[AFL_LIMIT_STRING_LEN];
static char name_strbuf[AFL_LIMIT_STRING_LEN];
static char varname_strbuf[AFL_LIMIT_STRING_LEN];
static bson_t* selector;
static bson_t* metadata;
//<-
static gboolean
build_selector_single(guint varname, guint value)
{
	gboolean ret_expected = TRUE;
	bson_t bson_child;
	selector = bson_new();
	sprintf(varname_strbuf, AFL_VARNAME_FORMAT, varname);
	MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
	bson_append_document_begin(selector, "0", -1, &bson_child);
	MYABORT_IF(!bson_append_int32(&bson_child, "_operator", -1, J_DB_OPERATOR_EQ));
	MYABORT_IF(!bson_append_utf8(&bson_child, "_name", -1, varname_strbuf, -1));
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		switch (namespace_vartypes[random_values.namespace][random_values.name][varname])
		{
		case J_DB_TYPE_SINT32:
			MYABORT_IF(!bson_append_int32(&bson_child, "_value", -1, value));
			break;
		case J_DB_TYPE_UINT32:
			MYABORT_IF(!bson_append_int32(&bson_child, "_value", -1, value));
			break;
		case J_DB_TYPE_FLOAT32:
			MYABORT_IF(!bson_append_double(&bson_child, "_value", -1, value));
			break;
		case J_DB_TYPE_SINT64:
			MYABORT_IF(!bson_append_int64(&bson_child, "_value", -1, value));
			break;
		case J_DB_TYPE_UINT64:
			MYABORT_IF(!bson_append_int64(&bson_child, "_value", -1, value));
			break;
		case J_DB_TYPE_FLOAT64:
			MYABORT_IF(!bson_append_double(&bson_child, "_value", -1, value));
			break;
		case J_DB_TYPE_STRING:
			if (value == AFL_LIMIT_SCHEMA_VALUES)
			{
				MYABORT_IF(!bson_append_utf8(&bson_child, "_value", -1, "not_existent_var_name", -1));
			}
			else
			{
				MYABORT_IF(!bson_append_utf8(&bson_child, "_value", -1, namespace_varvalues_string_const[value % AFL_LIMIT_SCHEMA_STRING_VALUES], -1));
			}
			break;
		case J_DB_TYPE_BLOB:
			if (value == AFL_LIMIT_SCHEMA_VALUES)
			{
				MYABORT_IF(!bson_append_binary(&bson_child, "_value", BSON_SUBTYPE_BINARY, -1, (const uint8_t*)"not_existent_var_name", 1 + strlen("not_existent_var_name")));
			}
			else
			{
				MYABORT_IF(!bson_append_binary(&bson_child, "_value", BSON_SUBTYPE_BINARY, -1, (const uint8_t*)namespace_varvalues_string_const[value % AFL_LIMIT_SCHEMA_STRING_VALUES], 1 + strlen(namespace_varvalues_string_const[value % AFL_LIMIT_SCHEMA_STRING_VALUES])));
			}
			break;
		case _J_DB_TYPE_COUNT:
			MYABORT_DEFAULT();
		}
	}
	else
	{ //operation on not existent namespace
		ret_expected = FALSE;
		MYABORT_IF(!bson_append_int32(&bson_child, "_value", -1, value));
	}
	bson_append_document_end(selector, &bson_child);
	J_AFL_DEBUG_BSON(selector);
	return ret_expected;
}
static gboolean
build_metadata(void)
{
	bson_t* bson;
	gboolean ret_expected = TRUE;
	guint count = 0;
	guint i;
	if (random_values.values.invalid_bson_metadata % 3)
	{
		switch (random_values.values.invalid_bson_metadata % 3)
		{
		case 2: //empty metadata
			metadata = bson_new();
			return FALSE;
		case 1: //NULL metadata
			metadata = NULL;
			return FALSE;
		case 0:
		default:;
		}
	}
	metadata = bson_new();
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		random_values.values.value_index = random_values.values.value_index % AFL_LIMIT_SCHEMA_VALUES;
		random_values.values.value_count = namespace_varcount[random_values.namespace][random_values.name] + ((random_values.values.value_count % 3) - 1);
		namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][0] = random_values.values.value_index;
		namespace_varvalues_double[random_values.namespace][random_values.name][random_values.values.value_index][0] = random_values.values.value_index;
		namespace_varvalues_string[random_values.namespace][random_values.name][random_values.values.value_index][0] = random_values.values.value_index % AFL_LIMIT_SCHEMA_STRING_VALUES;
		for (i = 1; i < AFL_LIMIT_SCHEMA_VARIABLES; i++)
		{
			namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i] = random_values.values.values[i][0].i;
			namespace_varvalues_double[random_values.namespace][random_values.name][random_values.values.value_index][i] = random_values.values.values[i][0].d;
			namespace_varvalues_string[random_values.namespace][random_values.name][random_values.values.value_index][i] = random_values.values.values[i][0].s % AFL_LIMIT_SCHEMA_STRING_VALUES;
		}
		bson = metadata;
		for (i = 0; i < AFL_LIMIT_SCHEMA_VARIABLES; i++)
		{
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, i);
			if (i < random_values.values.value_count)
			{
				if (i < namespace_varcount[random_values.namespace][random_values.name])
				{
					switch (namespace_vartypes[random_values.namespace][random_values.name][i])
					{
					case J_DB_TYPE_SINT32:
						count++;
						MYABORT_IF(!bson_append_int32(bson, varname_strbuf, -1, (gint32)namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i]));
						break;
					case J_DB_TYPE_UINT32:
						count++;
						MYABORT_IF(!bson_append_int32(bson, varname_strbuf, -1, (guint32)namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i]));
						break;
					case J_DB_TYPE_FLOAT32:
						count++;
						MYABORT_IF(!bson_append_double(bson, varname_strbuf, -1, (gfloat)namespace_varvalues_double[random_values.namespace][random_values.name][random_values.values.value_index][i]));
						break;
					case J_DB_TYPE_SINT64:
						count++;
						MYABORT_IF(!bson_append_int64(bson, varname_strbuf, -1, (gint64)namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i]));
						break;
					case J_DB_TYPE_UINT64:
						count++;
						MYABORT_IF(!bson_append_int64(bson, varname_strbuf, -1, (guint64)namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i]));
						break;
					case J_DB_TYPE_FLOAT64:
						count++;
						MYABORT_IF(!bson_append_double(bson, varname_strbuf, -1, (gdouble)namespace_varvalues_double[random_values.namespace][random_values.name][random_values.values.value_index][i]));
						break;
					case J_DB_TYPE_STRING:
						count++;
						MYABORT_IF(!bson_append_utf8(bson, varname_strbuf, -1, namespace_varvalues_string_const[namespace_varvalues_string[random_values.namespace][random_values.name][random_values.values.value_index][i]], -1));
						break;
					case J_DB_TYPE_BLOB:
						count++;
						MYABORT_IF(!bson_append_binary(bson, varname_strbuf, -1, BSON_SUBTYPE_BINARY, (const uint8_t*)namespace_varvalues_string_const[namespace_varvalues_string[random_values.namespace][random_values.name][random_values.values.value_index][i]], 1 + strlen(namespace_varvalues_string_const[namespace_varvalues_string[random_values.namespace][random_values.name][random_values.values.value_index][i]])));
						break;
					case _J_DB_TYPE_COUNT:
						MYABORT_DEFAULT();
					}
				}
				else
				{
					//TODO test other types for the invalid extra column - should fail on the column name anyway
					sprintf(varname_strbuf, AFL_VARNAME_FORMAT, AFL_LIMIT_SCHEMA_VARIABLES);
					bson_append_int32(bson, varname_strbuf, -1, 1); //not existent varname
					ret_expected = FALSE;
				}
			}
		}
	}
	else
	{
		sprintf(varname_strbuf, AFL_VARNAME_FORMAT, AFL_LIMIT_SCHEMA_VARIABLES);
		bson_append_int32(metadata, varname_strbuf, -1, 1); //not existent namespace & not existent varname
		ret_expected = FALSE;
	}
	J_AFL_DEBUG_BSON(metadata);
	return ret_expected && count;
}
static void
event_query_single(void)
{
	uint32_t binary_len;
	const uint8_t* binary;
	GError* error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	bson_t bson;
	bson_t bson_child;
	bson_t bson_child2;
	bson_iter_t iter;
	guint i;
	gboolean ret;
	gboolean ret_expected = TRUE;
	gpointer iterator = 0;
	if (random_values.values.invalid_bson_selector % 8)
	{
		switch (random_values.values.invalid_bson_selector % 8)
		{
		case 7: //NULL name
			build_selector_single(0, random_values.values.value_index);
			ret = j_db_internal_query(namespace_strbuf, NULL, selector, &iterator, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			ret = j_db_internal_iterate(iterator, &bson, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 6: //NULL namespace
			build_selector_single(0, random_values.values.value_index);
			ret = j_db_internal_query(NULL, name_strbuf, selector, &iterator, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			ret = j_db_internal_iterate(iterator, &bson, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 5: //NULL iterator
			build_selector_single(0, random_values.values.value_index);
			ret = j_db_internal_query(namespace_strbuf, name_strbuf, selector, NULL, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 4: //invalid bson - value of not allowed bson type
			selector = bson_new();
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_document_begin(&bson_child, "_value", -1, &bson_child2);
			bson_append_document_end(&bson_child, &bson_child2);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_query(namespace_strbuf, name_strbuf, selector, &iterator, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			ret = j_db_internal_iterate(iterator, &bson, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 3: //invalid bson - operator undefined enum
			selector = bson_new();
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_int32(&bson_child, "operator", -1, _J_DB_OPERATOR_COUNT + 1);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_query(namespace_strbuf, name_strbuf, selector, &iterator, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			ret = j_db_internal_iterate(iterator, &bson, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 2: //invalid bson - operator of invalid type
			selector = bson_new();
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_document_begin(&bson_child, "operator", -1, &bson_child2);
			bson_append_document_end(&bson_child, &bson_child2);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_query(namespace_strbuf, name_strbuf, selector, &iterator, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			ret = j_db_internal_iterate(iterator, &bson, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 1: //invalid bson - key of type something else than a document
			selector = bson_new();
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
			bson_append_int32(selector, varname_strbuf, -1, 0);
			ret = j_db_internal_query(namespace_strbuf, name_strbuf, selector, &iterator, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			ret = j_db_internal_iterate(iterator, &bson, &error);
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 0:
		default:;
		}
		if (selector)
		{
			bson_destroy(selector);
			selector = NULL;
		}
	}
	random_values.values.existent = random_values.values.existent % 2;
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		random_values.values.value_index = random_values.values.value_index % AFL_LIMIT_SCHEMA_VALUES;
		ret_expected = namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index] && ret_expected;
		if (random_values.values.existent)
		{
			ret_expected = build_selector_single(0, random_values.values.value_index) && ret_expected;
		}
		else
		{
			ret_expected = build_selector_single(0, AFL_LIMIT_SCHEMA_VALUES) && ret_expected;
			ret_expected = FALSE;
		}
	}
	else
	{
		ret_expected = FALSE;
	}
	ret = j_db_internal_query(namespace_strbuf, name_strbuf, selector, &iterator, batch, &error);
	ret = j_batch_execute(batch) && ret;
	J_AFL_DEBUG_ERROR_NO_EXPECT(ret, error);
	if (selector)
	{
		bson_destroy(selector);
		selector = NULL;
	}
	if (ret_expected)
	{
		ret = j_db_internal_iterate(iterator, &bson, &error);
		J_AFL_DEBUG_ERROR(ret, TRUE, error);
		MYABORT_IF(!bson_iter_init(&iter, &bson));
		MYABORT_IF(!bson_iter_find(&iter, "_id"));
		for (i = 0; i < AFL_LIMIT_SCHEMA_VARIABLES; i++)
		{
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, i);
			MYABORT_IF(!bson_iter_init(&iter, &bson));
			if (i < namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index])
			{
				MYABORT_IF(!bson_iter_find(&iter, varname_strbuf));
				switch (namespace_vartypes[random_values.namespace][random_values.name][i])
				{
				case J_DB_TYPE_SINT32:
					MYABORT_IF((gint32)bson_iter_int32(&iter) != (gint32)namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i]);
					break;
				case J_DB_TYPE_UINT32:
					MYABORT_IF((guint32)bson_iter_int32(&iter) != (guint32)namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i]);
					break;
				case J_DB_TYPE_FLOAT32:
					MYABORT_IF(!G_APPROX_VALUE((gfloat)bson_iter_double(&iter), (gfloat)namespace_varvalues_double[random_values.namespace][random_values.name][random_values.values.value_index][i], 0.001f));
					break;
				case J_DB_TYPE_SINT64:
					MYABORT_IF((gint64)bson_iter_int64(&iter) != (gint64)namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i]);
					break;
				case J_DB_TYPE_UINT64:
					MYABORT_IF((guint64)bson_iter_int64(&iter) != (guint64)namespace_varvalues_int64[random_values.namespace][random_values.name][random_values.values.value_index][i]);
					break;
				case J_DB_TYPE_FLOAT64:
					MYABORT_IF(!G_APPROX_VALUE((gdouble)bson_iter_double(&iter), (gdouble)namespace_varvalues_double[random_values.namespace][random_values.name][random_values.values.value_index][i], 0.001));
					break;
				case J_DB_TYPE_STRING:
					MYABORT_IF(g_strcmp0(bson_iter_utf8(&iter, NULL), namespace_varvalues_string_const[namespace_varvalues_string[random_values.namespace][random_values.name][random_values.values.value_index][i]]));
					break;
				case J_DB_TYPE_BLOB:
					if (bson_iter_type(&iter) == BSON_TYPE_BINARY)
					{
						bson_iter_binary(&iter, NULL, &binary_len, &binary);
						MYABORT_IF(g_strcmp0((const char*)binary, namespace_varvalues_string_const[namespace_varvalues_string[random_values.namespace][random_values.name][random_values.values.value_index][i]]));
					}
					else
						MYABORT_IF(bson_iter_type(&iter) != BSON_TYPE_NULL);
					break;
				case _J_DB_TYPE_COUNT:
					MYABORT_DEFAULT();
				}
			}
			else if (i < namespace_varcount[random_values.namespace][random_values.name])
			{
				//undefined result - since variable was NOT written to DB, but variable was declared in the schema
			}
			else
			{
				MYABORT_IF(bson_iter_find(&iter, varname_strbuf));
			}
		}
		bson_destroy(&bson);
		selector = NULL;
	}
	ret = j_db_internal_iterate(iterator, &bson, &error);
	J_AFL_DEBUG_ERROR(ret, FALSE, error);
}
static void
event_query_all(void)
{
	guint i;
	//TODO query all at once
	for (i = 0; i < AFL_LIMIT_SCHEMA_VALUES; i++)
	{
		if (namespace_varvalues_valid[random_values.namespace][random_values.name][i])
		{
			random_values.values.value_index = i;
			event_query_single();
		}
	}
}
static void
event_delete(void)
{
	GError* error = NULL;
	//TODO delete ALL at once using empty bson and NULL
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	bson_t bson_child;
	bson_t bson_child2;
	gboolean ret;
	gboolean ret_expected = TRUE;
	if (random_values.values.invalid_bson_selector % 7)
	{
		switch (random_values.values.invalid_bson_selector % 7)
		{
		case 6: //NULL name
			build_selector_single(0, random_values.values.value_index);
			ret = j_db_internal_delete(namespace_strbuf, NULL, selector, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 5: //NULL namespace
			build_selector_single(0, random_values.values.value_index);
			ret = j_db_internal_delete(NULL, name_strbuf, selector, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 4: //invalid bson - value of not allowed bson type
			selector = bson_new();
			MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_document_begin(&bson_child, "_value", -1, &bson_child2);
			bson_append_document_end(&bson_child, &bson_child2);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_delete(namespace_strbuf, name_strbuf, selector, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 3: //invalid bson - operator undefined enum
			selector = bson_new();
			MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_int32(&bson_child, "operator", -1, _J_DB_OPERATOR_COUNT + 1);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_delete(namespace_strbuf, name_strbuf, selector, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 2: //invalid bson - operator of invalid type
			selector = bson_new();
			MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_document_begin(&bson_child, "operator", -1, &bson_child2);
			bson_append_document_end(&bson_child, &bson_child2);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_delete(namespace_strbuf, name_strbuf, selector, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 1: //invalid bson - key of type something else than a document
			selector = bson_new();
			MYABORT_IF(!bson_append_int32(selector, "_mode", -1, J_DB_SELECTOR_MODE_AND));
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson_append_int32(selector, varname_strbuf, -1, 0);
			ret = j_db_internal_delete(namespace_strbuf, name_strbuf, selector, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 0:
		default:;
		}
		if (selector)
		{
			bson_destroy(selector);
			selector = NULL;
		}
	}
	random_values.values.existent = random_values.values.existent % 2;
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		random_values.values.value_index = random_values.values.value_index % AFL_LIMIT_SCHEMA_VALUES;
		if (random_values.values.existent)
		{
			ret_expected = build_selector_single(0, random_values.values.value_index) && ret_expected; //selector only contains valid columns
			ret_expected = namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index] && ret_expected; //row exists before ?
			namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index] = 0;
		}
		else
		{
			ret_expected = build_selector_single(0, AFL_LIMIT_SCHEMA_VALUES) && ret_expected; //row does not exist before
			ret_expected = FALSE;
		}
	}
	else
	{
		ret_expected = FALSE;
	}
	ret = j_db_internal_delete(namespace_strbuf, name_strbuf, selector, batch, &error);
	ret = j_batch_execute(batch) && ret;
	J_AFL_DEBUG_ERROR(ret, ret_expected, error);
	if (selector)
	{
		bson_destroy(selector);
		selector = NULL;
	}
	selector = NULL;
}
static void
event_insert(void)
{
	GError* error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	gboolean ret;
	gboolean ret_expected = TRUE;
	ret_expected = build_metadata() && ret_expected; //inserting valid metadata should succeed
	if (random_values.values.invalid_bson_selector % 3)
	{
		switch (random_values.values.invalid_bson_selector % 3)
		{
		case 2: //NULL name
			ret = j_db_internal_insert(namespace_strbuf, NULL, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 1: //NULL namespace
			ret = j_db_internal_insert(NULL, name_strbuf, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 0:
		default:;
		}
	}
	ret = j_db_internal_insert(namespace_strbuf, name_strbuf, metadata, batch, &error);
	ret = j_batch_execute(batch) && ret;
	J_AFL_DEBUG_ERROR(ret, ret_expected, error);
	if (ret)
	{
		namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index] = random_values.values.value_count;
	}
	else
	{
		if (namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index])
			namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index] = 1;
	}
	if (metadata)
	{
		bson_destroy(metadata);
		metadata = NULL;
	}
}
static void
event_update(void)
{
	//TODO update multile rows together
	//TODO selector useing AND/OR query elements
	GError* error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	bson_t bson_child;
	bson_t bson_child2;
	gboolean ret;
	gboolean ret_expected = TRUE;
	if (random_values.values.invalid_bson_selector % 9)
	{
		build_metadata();
		switch (random_values.values.invalid_bson_selector % 9)
		{
		case 8: //NULL name
			build_selector_single(0, random_values.values.value_index);
			ret = j_db_internal_update(namespace_strbuf, NULL, selector, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 7: //NULL namespace
			build_selector_single(0, random_values.values.value_index);
			ret = j_db_internal_update(NULL, name_strbuf, selector, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 6: //invalid bson - value of not allowed bson type
			selector = bson_new();
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_document_begin(&bson_child, "_value", -1, &bson_child2);
			bson_append_document_end(&bson_child, &bson_child2);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_update(namespace_strbuf, name_strbuf, selector, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 5: //invalid bson - operator undefined enum
			selector = bson_new();
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_int32(&bson_child, "operator", -1, _J_DB_OPERATOR_COUNT + 1);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_update(namespace_strbuf, name_strbuf, selector, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 4: //invalid bson - operator of invalid type
			selector = bson_new();
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson_append_document_begin(selector, varname_strbuf, -1, &bson_child);
			bson_append_document_begin(&bson_child, "operator", -1, &bson_child2);
			bson_append_document_end(&bson_child, &bson_child2);
			bson_append_document_end(selector, &bson_child);
			ret = j_db_internal_update(namespace_strbuf, name_strbuf, selector, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 3: //invalid bson - key of type something else than a document
			selector = bson_new();
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson_append_int32(selector, varname_strbuf, -1, 0);
			ret = j_db_internal_update(namespace_strbuf, name_strbuf, selector, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 2: //empty bson
			selector = bson_new();
			ret = j_db_internal_update(namespace_strbuf, name_strbuf, selector, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 1: //NULL selector
			ret = j_db_internal_update(namespace_strbuf, name_strbuf, NULL, metadata, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 0:
		default:;
		}
		if (selector)
		{
			bson_destroy(selector);
			selector = NULL;
		}
		if (metadata)
		{
			bson_destroy(metadata);
			metadata = NULL;
		}
	}
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		random_values.values.value_index = random_values.values.value_index % AFL_LIMIT_SCHEMA_VALUES;
		if (random_values.values.existent)
		{
			ret_expected = build_selector_single(0, random_values.values.value_index) && ret_expected; //update a (maybe) existent row
			ret_expected = namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index] && ret_expected; //row to update exists
		}
		else
		{
			ret_expected = build_selector_single(0, AFL_LIMIT_SCHEMA_VALUES) && ret_expected; //update a definetly not existing row
			ret_expected = FALSE;
		}
	}
	else
	{
		ret_expected = FALSE; //operation on not existent namespace must fail
		selector = bson_new();
	}
	ret_expected = build_metadata() && ret_expected; //metadata contains valid data ?
	ret = j_db_internal_update(namespace_strbuf, name_strbuf, selector, metadata, batch, &error);
	ret = j_batch_execute(batch) && ret;
	J_AFL_DEBUG_ERROR(ret, ret_expected, error);
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		if (ret)
			namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index] = random_values.values.value_count;
		else if (namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index])
			namespace_varvalues_valid[random_values.namespace][random_values.name][random_values.values.value_index] = 1;
	}
	if (selector)
	{
		bson_destroy(selector);
		selector = NULL;
	}
	if (metadata)
	{
		bson_destroy(metadata);
		metadata = NULL;
	}
}
static void
event_schema_get(void)
{
	GError* error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	bson_iter_t iter;
	gboolean ret;
	gboolean ret_expected;
	bson_t bson;
	guint i;
	if (random_values.schema_create.invalid_bson_schema % 4)
	{
		switch (random_values.schema_create.invalid_bson_schema % 4)
		{
		case 3: //NULL name
			ret = j_db_internal_schema_get(namespace_strbuf, NULL, &bson, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 2: //NULL namespace
			ret = j_db_internal_schema_get(NULL, name_strbuf, &bson, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 1: //empty schema
			ret = j_db_internal_schema_get(namespace_strbuf, name_strbuf, NULL, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, namespace_exist[random_values.namespace][random_values.name], error);
			break;
		case 0:
		default:;
		}
	}
	ret_expected = namespace_exist[random_values.namespace][random_values.name];
	ret = j_db_internal_schema_get(namespace_strbuf, name_strbuf, &bson, batch, &error);
	ret = j_batch_execute(batch) && ret;
	J_AFL_DEBUG_ERROR(ret, ret_expected, error);
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		if (ret)
		{
			J_AFL_DEBUG_BSON(&bson);
			for (i = 0; i < AFL_LIMIT_SCHEMA_VARIABLES; i++)
			{
				sprintf(varname_strbuf, AFL_VARNAME_FORMAT, i);
				MYABORT_IF(!bson_iter_init(&iter, &bson));
				if (i < namespace_varcount[random_values.namespace][random_values.name])
				{
					MYABORT_IF(!bson_iter_find(&iter, varname_strbuf));
					MYABORT_IF(!BSON_ITER_HOLDS_INT32(&iter));
					MYABORT_IF(namespace_vartypes[random_values.namespace][random_values.name][i] != (JDBType)bson_iter_int32(&iter));
				}
				else
				{
					MYABORT_IF(bson_iter_find(&iter, varname_strbuf));
				}
			}
		}
	}
	if (ret)
		bson_destroy(&bson);
}
static void
event_schema_delete(void)
{
	GError* error = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	gboolean ret;
	if (random_values.schema_create.invalid_bson_schema % 3)
	{
		switch (random_values.schema_create.invalid_bson_schema % 3)
		{
		case 2: //NULL name
			ret = j_db_internal_schema_delete(namespace_strbuf, NULL, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 1: //NULL namespace
			ret = j_db_internal_schema_delete(NULL, name_strbuf, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 0:
		default:;
		}
	}
	ret = j_db_internal_schema_delete(namespace_strbuf, name_strbuf, batch, &error);
	ret = j_batch_execute(batch) && ret;
	J_AFL_DEBUG_ERROR_NO_EXPECT(ret, error);
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		MYABORT_IF(!ret);
		if (namespace_bson[random_values.namespace][random_values.name])
			bson_destroy(namespace_bson[random_values.namespace][random_values.name]);
		namespace_bson[random_values.namespace][random_values.name] = NULL;
		namespace_exist[random_values.namespace][random_values.name] = FALSE;
	}
	else
	{
		MYABORT_IF(ret);
	}
}
static void
event_schema_create(void)
{
	GError* error = NULL;
	//TODO test create index
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	bson_t bson_child;
	gboolean ret;
	gboolean ret_expected;
	bson_t* bson;
	guint i;
	if (random_values.schema_create.invalid_bson_schema % 7)
	{
		switch (random_values.schema_create.invalid_bson_schema % 7)
		{
		case 6: //NULL name
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson = bson_new();
			bson_append_int32(bson, varname_strbuf, -1, J_DB_TYPE_UINT32);
			ret = j_db_internal_schema_create(namespace_strbuf, NULL, bson, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			bson_destroy(bson);
			break;
		case 5: //NULL namespace
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson = bson_new();
			bson_append_int32(bson, varname_strbuf, -1, J_DB_TYPE_UINT32);
			ret = j_db_internal_schema_create(NULL, name_strbuf, bson, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			bson_destroy(bson);
			break;
		case 4: //variable type not specified in enum
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson = bson_new();
			bson_append_int32(bson, varname_strbuf, -1, _J_DB_TYPE_COUNT + 1);
			ret = j_db_internal_schema_create(namespace_strbuf, name_strbuf, bson, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			bson_destroy(bson);
			break;
		case 3: //wrong bson variable types
			sprintf(varname_strbuf, AFL_VARNAME_FORMAT, 0);
			bson = bson_new();
			bson_append_document_begin(bson, varname_strbuf, -1, &bson_child);
			bson_append_document_end(bson, &bson_child);
			ret = j_db_internal_schema_create(namespace_strbuf, name_strbuf, bson, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			bson_destroy(bson);
			break;
		case 2: //empty bson
			bson = bson_new();
			ret = j_db_internal_schema_create(namespace_strbuf, name_strbuf, bson, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			bson_destroy(bson);
			break;
		case 1: //NULL
			ret = j_db_internal_schema_create(namespace_strbuf, name_strbuf, NULL, batch, &error);
			ret = j_batch_execute(batch) && ret;
			J_AFL_DEBUG_ERROR(ret, FALSE, error);
			break;
		case 0:
			MYABORT_DEFAULT();
		}
	}
	random_values.schema_create.duplicate_variables = random_values.schema_create.duplicate_variables % 2;
	random_values.schema_create.variable_count = random_values.schema_create.variable_count % AFL_LIMIT_SCHEMA_VARIABLES;
	for (i = 0; i < random_values.schema_create.variable_count; i++)
		random_values.schema_create.variable_types[i] = random_values.schema_create.variable_types[i] % _J_DB_TYPE_COUNT;
	bson = bson_new();
	ret_expected = random_values.schema_create.variable_count > 0;
	if (random_values.schema_create.variable_types[0] == J_DB_TYPE_BLOB)
		random_values.schema_create.variable_types[0] = J_DB_TYPE_UINT32;
	for (i = 0; i < random_values.schema_create.variable_count; i++)
	{
		sprintf(varname_strbuf, AFL_VARNAME_FORMAT, i);
		if (random_values.schema_create.variable_types[i] == _J_DB_TYPE_COUNT)
		{
			ret_expected = FALSE; //all specified types must be valid
		}
		bson_append_int32(bson, varname_strbuf, -1, random_values.schema_create.variable_types[i]);
		if (i == 0 && random_values.schema_create.duplicate_variables)
		{
			bson_append_int32(bson, varname_strbuf, -1, random_values.schema_create.variable_types[i]);
			ret_expected = FALSE; //duplicate variable names not allowed
		}
	}
	ret_expected = !namespace_exist[random_values.namespace][random_values.name] && ret_expected;
	J_AFL_DEBUG_BSON(bson);
	ret = j_db_internal_schema_create(namespace_strbuf, name_strbuf, bson, batch, &error);
	ret = j_batch_execute(batch) && ret;
	J_AFL_DEBUG_ERROR(ret, ret_expected, error);
	if (namespace_exist[random_values.namespace][random_values.name])
	{
		if (bson)
			bson_destroy(bson);
	}
	else
	{
		if (ret)
		{
			namespace_exist[random_values.namespace][random_values.name] = TRUE;
			namespace_bson[random_values.namespace][random_values.name] = bson;
			namespace_varcount[random_values.namespace][random_values.name] = random_values.schema_create.variable_count;
			for (i = 0; i < random_values.schema_create.variable_count; i++)
				namespace_vartypes[random_values.namespace][random_values.name][i] = random_values.schema_create.variable_types[i];
			for (i = 0; i < AFL_LIMIT_SCHEMA_VALUES; i++)
				namespace_varvalues_valid[random_values.namespace][random_values.name][i] = 0;
		}
		else
		{
			if (bson)
				bson_destroy(bson);
		}
	}
}
int
main(int argc, char* argv[])
{
	FILE* file;
	JDBAflEvent event;
	guint i, j, k;
	guint tmp;
	if (argc > 1)
	{
		char filename[50 + strlen(argv[1])];
		mkdir(argv[1], S_IRUSR | S_IRGRP | S_IROTH);
		sprintf(filename, "%s/start-files", argv[1]);
		mkdir(filename, S_IRUSR | S_IRGRP | S_IROTH);
		memset(&random_values, 0, sizeof(random_values));
		for (i = 0; i < _AFL_EVENT_DB_COUNT; i++)
		{
			sprintf(filename, "%s/start-files/test-db-backend-%d.bin", argv[1], i);
			file = fopen(filename, "wb");
			event = i;
			fwrite(&event, sizeof(event), 1, file);
			fwrite(&random_values, sizeof(random_values), 1, file);
			fclose(file);
		}
		goto fini;
	}
	for (i = 0; i < AFL_LIMIT_SCHEMA_STRING_VALUES; i++)
	{
		sprintf(&namespace_varvalues_string_const[i][0], AFL_STRING_CONST_FORMAT, i);
	}
	for (i = 0; i < AFL_LIMIT_SCHEMA_NAMESPACE; i++)
	{
		for (j = 0; j < AFL_LIMIT_SCHEMA_NAME; j++)
		{
			namespace_exist[i][j] = FALSE;
			namespace_varcount[i][j] = 0;
			namespace_bson[i][j] = NULL;
			for (k = 0; k < AFL_LIMIT_SCHEMA_VARIABLES; k++)
			{
				namespace_vartypes[i][j][k] = _J_DB_TYPE_COUNT;
			}
		}
	}
#ifdef __AFL_HAVE_MANUAL_CONTROL
	//https://github.com/mirrorer/afl/tree/master/llvm_mode
	// this does not work with threads or network connections
	//        __AFL_INIT();
	//      while (__AFL_LOOP(1000))
#endif
	{
	loop:
		MY_READ_MAX(event, _AFL_EVENT_DB_COUNT);
		MY_READ(random_values);
		random_values.namespace = random_values.namespace % AFL_LIMIT_SCHEMA_NAMESPACE;
		random_values.name = random_values.name % AFL_LIMIT_SCHEMA_NAME;
		sprintf(namespace_strbuf, AFL_NAMESPACE_FORMAT, random_values.namespace);
		sprintf(name_strbuf, AFL_NAME_FORMAT, random_values.name);
		switch (event)
		{
		case AFL_EVENT_DB_SCHEMA_CREATE:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_CREATE %s %s", namespace_strbuf, name_strbuf);
			event_schema_get();
			event_schema_create();
			event_schema_get();
			break;
		case AFL_EVENT_DB_SCHEMA_GET:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_GET %s %s", namespace_strbuf, name_strbuf);
			event_schema_get();
			break;
		case AFL_EVENT_DB_SCHEMA_DELETE:
			J_DEBUG("AFL_EVENT_DB_SCHEMA_DELETE %s %s", namespace_strbuf, name_strbuf);
			event_schema_get();
			event_schema_delete();
			event_schema_get();
			break;
		case AFL_EVENT_DB_INSERT:
			J_DEBUG("AFL_EVENT_DB_INSERT %s %s", namespace_strbuf, name_strbuf);
			random_values.values.existent = 1;
			tmp = random_values.values.value_index % (AFL_LIMIT_SCHEMA_VALUES);
			event_query_all();
			random_values.values.value_index = tmp;
			event_delete();
			event_query_all();
			random_values.values.value_index = tmp;
			event_insert();
			event_query_all();
			break;
		case AFL_EVENT_DB_UPDATE:
			J_DEBUG("AFL_EVENT_DB_UPDATE %s %s", namespace_strbuf, name_strbuf);
			event_query_all();
			event_update();
			event_query_all();
			break;
		case AFL_EVENT_DB_DELETE:
			J_DEBUG("AFL_EVENT_DB_DELETE %s %s", namespace_strbuf, name_strbuf);
			event_query_all();
			event_delete();
			event_query_all();
			break;
		case AFL_EVENT_DB_QUERY_ALL:
			J_DEBUG("AFL_EVENT_DB_QUERY_ALL %s %s", namespace_strbuf, name_strbuf);
			event_query_all();
			event_query_single();
			break;
		case _AFL_EVENT_DB_COUNT:
			MYABORT_DEFAULT();
		}
		goto loop;
	cleanup:
		for (i = 0; i < AFL_LIMIT_SCHEMA_NAMESPACE; i++)
		{
			for (j = 0; j < AFL_LIMIT_SCHEMA_NAME; j++)
			{
				if (namespace_bson[i][j])
					bson_destroy(namespace_bson[i][j]);
				namespace_bson[i][j] = NULL;
			}
		}
	}
fini:
	j_fini(); //memory leaks count as error -> free everything possible
	return 0;
}
