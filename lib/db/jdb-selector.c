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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <bson.h>

#include <julea.h>
#include <db/jdb-internal.h>
#include <julea-db.h>
#include "../../backend/db/jbson.c"

JDBSelector*
j_db_selector_new(JDBSchema* schema, JDBSelectorMode mode, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue val;
	JDBSelector* selector = NULL;

	g_return_val_if_fail(error == NULL || *error == NULL, NULL);

	selector = j_helper_alloc_aligned(128, sizeof(JDBSelector));
	selector->ref_count = 1;
	selector->mode = mode;
	selector->bson_count = 0;
	selector->join_schema = NULL;
	selector->join_schema_count = 0;
	bson_init(&selector->bson);
	selector->schema = j_db_schema_ref(schema);

	if (G_UNLIKELY(!selector->schema))
	{
		goto _error;
	}

	val.val_uint32 = mode;

	if (G_UNLIKELY(!j_bson_append_value(&selector->bson, "_mode", J_DB_TYPE_UINT32, &val, error)))
	{
		goto _error;
	}

	return selector;

_error:
	j_db_selector_unref(selector);
	return NULL;
}

JDBSelector*
j_db_selector_ref(JDBSelector* selector)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(selector != NULL, NULL);

	g_atomic_int_inc(&selector->ref_count);

	return selector;
}

void
j_db_selector_unref(JDBSelector* selector)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(selector != NULL);

	if (g_atomic_int_dec_and_test(&selector->ref_count))
	{
		for (guint i = 0; i < selector->join_schema_count; i++)
		{
			j_db_schema_unref(selector->join_schema[i]);
		}
		g_free(selector->join_schema);

		j_db_schema_unref(selector->schema);
		bson_destroy(&selector->bson);
		g_free(selector);
	}
}

gboolean
j_db_selector_add_field(JDBSelector* selector, gchar const* name, JDBSelectorOperator operator_, gconstpointer value, guint64 length, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	char buf[20];
	bson_t bson;
	JDBType type;
	JDBTypeValue val;

	GString* table_name = g_string_new(NULL);
	g_string_append_printf(table_name, "%s_%s", selector->schema->namespace, selector->schema->name);

	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	if (G_UNLIKELY(selector->bson_count + 1 > 500))
	{
		g_set_error_literal(error, J_DB_ERROR, J_DB_ERROR_SELECTOR_TOO_COMPLEX, "selector too complex");
		goto _error;
	}

	if (G_UNLIKELY(!j_db_schema_get_field(selector->schema, name, &type, error)))
	{
		goto _error;
	}

	snprintf(buf, sizeof(buf), "%d", selector->bson_count);

	if (G_UNLIKELY(!j_bson_append_document_begin(&selector->bson, buf, &bson, error)))
	{
		goto _error;
	}

	val.val_string = table_name->str;

	if (G_UNLIKELY(!j_bson_append_value(&bson, "_table", J_DB_TYPE_STRING, &val, error)))
	{
		goto _error;
	}

	val.val_string = name;

	if (G_UNLIKELY(!j_bson_append_value(&bson, "_name", J_DB_TYPE_STRING, &val, error)))
	{
		goto _error;
	}

	val.val_uint32 = operator_;

	if (G_UNLIKELY(!j_bson_append_value(&bson, "_operator", J_DB_TYPE_UINT32, &val, error)))
	{
		goto _error;
	}

	switch (type)
	{
		case J_DB_TYPE_SINT32:
			val.val_sint32 = *(gint32 const*)value;
			break;
		case J_DB_TYPE_UINT32:
			val.val_uint32 = *(guint32 const*)value;
			break;
		case J_DB_TYPE_FLOAT32:
			val.val_float32 = *(gfloat const*)value;
			break;
		case J_DB_TYPE_SINT64:
			val.val_sint64 = *(gint64 const*)value;
			break;
		case J_DB_TYPE_UINT64:
			val.val_sint64 = *(gint64 const*)value;
			break;
		case J_DB_TYPE_FLOAT64:
			val.val_float64 = *(gdouble const*)value;
			break;
		case J_DB_TYPE_STRING:
			val.val_string = value;
			break;
		case J_DB_TYPE_BLOB:
			val.val_blob = value;
			val.val_blob_length = length;
			break;
		case J_DB_TYPE_ID:
		default:
			g_assert_not_reached();
	}

	if (G_UNLIKELY(!j_bson_append_value(&bson, "_value", type, &val, error)))
	{
		goto _error;
	}

	if (G_UNLIKELY(!j_bson_append_document_end(&selector->bson, &bson, error)))
	{
		goto _error;
	}

	selector->bson_count++;

	g_string_free(table_name, TRUE);

	return TRUE;

_error:
	g_string_free(table_name, TRUE);

	return FALSE;
}

void
j_db_selector_sync_schemas_for_join(JDBSelector* selector, JDBSelector* sub_selector)
{
	gboolean new_schema = true;

	if (selector == sub_selector)
	{
		// Ignore and return as same selector is passed as "selector" and "sub_selector" (mistakenly).
		return;
	}

	/*
	 * If sub_selector belongs to a different schema then it indicates the request contains join operations therefore the details 
	 * of sub_selector (i.e. secondary schema) should be added to the selector (i.e. primary selector).
	 */
	for (guint i = 0; i < selector->join_schema_count; i++)
	{
		// Check if the schema of sub_selector has already been added to the selector.
		if (selector->join_schema[i] == sub_selector->schema)
		{
			new_schema = false;
			break;
		}
	}
	if (new_schema)
	{
		// Increment the counter.
		g_atomic_int_inc(&selector->join_schema_count);
		// Extending memory to add schema for sub_selector.
		selector->join_schema = g_realloc(selector->join_schema, sizeof(JDBSchema*) * selector->join_schema_count);
		// Add the schema of sub_selector to the selector.
		selector->join_schema[selector->join_schema_count - 1] = j_db_schema_ref(sub_selector->schema);
	}

	/*
	 * Move secondary schemas from sub_selector to the primary selector. 
	 * The secondary selector (i.e. sub_selector) might contains schemas' information that would be added to it when it was passed as
	 * a primary selector. Therefore, the case when the secondary selector further has secondary schemas, the following code snippet 
	 * moves them to the current primary selector (i.e. selector).
	 */
	for (guint i = 0; i < sub_selector->join_schema_count; i++)
	{
		guint j = 0;
		new_schema = true;
		for (; j < selector->join_schema_count; j++)
		{
			if (selector->join_schema[j] == sub_selector->join_schema[i])
			{
				// The schema instance already exists, stop and continue with the next schema.
				new_schema = false;
				break;
			}
		}
		if (new_schema)
		{
			// Increment the counter.
			g_atomic_int_inc(&selector->join_schema_count);
			// Extending memory to add schema for sub_selector.
			selector->join_schema = g_realloc(selector->join_schema, sizeof(JDBSchema*) * selector->join_schema_count);
			// Add the schema of sub_selector to the selector.
			selector->join_schema[selector->join_schema_count - 1] = j_db_schema_ref(sub_selector->join_schema[i]);
		}
	}
}

gboolean
j_db_selector_add_selector(JDBSelector* selector, JDBSelector* sub_selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	char buf[20];

	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(sub_selector != NULL, FALSE);
	g_return_val_if_fail(selector != sub_selector, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	// The secondary selector (i.e. sub_selector) should have objects/instances added to its BSON object.
	/*if (G_UNLIKELY(!sub_selector->bson_count))
	{
		g_set_error_literal(error, J_DB_ERROR, J_DB_ERROR_SELECTOR_EMPTY, "selector must not be emoty");
		goto _error;
	}*/

	// Validate the count for BSON objects/instances that they should not exceed the given limit.
	if (G_UNLIKELY(selector->bson_count + sub_selector->bson_count > 500))
	{
		g_set_error_literal(error, J_DB_ERROR, J_DB_ERROR_SELECTOR_TOO_COMPLEX, "selector too complex");
		goto _error;
	}

	// Extract the current count of BSON objects. It would be used as a key to add BSON object of sub_selector as a document to primary selector's BSON object.
	snprintf(buf, sizeof(buf), "%d", selector->bson_count);
	if (G_UNLIKELY(!j_bson_append_document(&selector->bson, buf, &sub_selector->bson, error)))
	{
		goto _error;
	}

	// Move schemas' information from secondary (sub_selector) to primary selector (i.e. selector)
	j_db_selector_sync_schemas_for_join(selector, sub_selector);

	selector->bson_count += sub_selector->bson_count;

	return TRUE;

_error:
	return FALSE;
}

gboolean
j_db_selector_finalize(JDBSelector* selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_t bson;
	JDBTypeValue val;
	GString* key_name = NULL;
	GString* key_namespace = NULL;
	
	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	/*
	 * In the following code snippet the table's information is added as a BSON array. 
	 * BSON array has a key named "tables", and the tables' names are added to it as BSON array-items. 
	 */

	if (G_UNLIKELY(!j_bson_append_array_begin(&selector->bson, "tables", &bson, error)))
	{
		goto _error;
	}

	// Add namespace for the primary selector.
	val.val_string = selector->schema->namespace;
	if (G_UNLIKELY(!j_bson_append_value(&bson, "namespace0", J_DB_TYPE_STRING, &val, error)))
	{
		goto _error;
	}

	// Add table name for the primary selector.
	val.val_string = selector->schema->name;
	if (G_UNLIKELY(!j_bson_append_value(&bson, "name0", J_DB_TYPE_STRING, &val, error)))
	{
		goto _error;
	}

	// This code snippet iterates through the data structure that maintains information regarding the secondary schemas and pushes their info to BSON objects.
	for (guint i = 0; i < selector->join_schema_count; i++)
	{

		key_name = g_string_new(NULL);
		key_namespace = g_string_new(NULL);

		g_string_append_printf(key_namespace, "namespace%d", i + 1);

		// Add namespace for the i-th secondary selector.
		val.val_string = selector->join_schema[i]->namespace;
		if (G_UNLIKELY(!j_bson_append_value(&bson, key_namespace->str, J_DB_TYPE_STRING, &val, error)))
		{
			goto _error;
		}

		g_string_append_printf(key_name, "name%d", i + 1);

		// Add table name for the i-th secondary selector.
		val.val_string = selector->join_schema[i]->name;
		if (G_UNLIKELY(!j_bson_append_value(&bson, key_name->str, J_DB_TYPE_STRING, &val, error)))
		{
			goto _error;
		}

		g_string_free(key_namespace, TRUE);
		g_string_free(key_name, TRUE);
	}

	// Finalize the BSON array.
	if (G_UNLIKELY(!j_bson_append_array_end(&selector->bson, &bson, error)))
	{
		goto _error;
	}

	if (!key_namespace)
	{
		g_string_free(key_namespace, TRUE);
	}
	if (!key_name)
	{
		g_string_free(key_name, TRUE);
	}

	return TRUE;
_error:
	if (!key_namespace)
	{
		g_string_free(key_namespace, TRUE);
	}
	if (!key_name)
	{
		g_string_free(key_name, TRUE);
	}
	return FALSE;
}

gboolean
j_db_selector_add_join(JDBSelector* selector, gchar const* selector_field, JDBSelector* sub_selector, gchar const* sub_selector_field, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_t bson;
	JDBTypeValue val;

	GString* selector_tablename = g_string_new(NULL);
	GString* sub_selector_tablename = g_string_new(NULL);

	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(sub_selector != NULL, FALSE);
	g_return_val_if_fail(selector != sub_selector, FALSE);
	g_return_val_if_fail(selector->schema != sub_selector->schema, TRUE);
	g_return_val_if_fail(selector_field != NULL, FALSE);
	g_return_val_if_fail(sub_selector_field != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	/*
	 * In the following code snippet the join information is added as a BSON document. 
	 * BSON document has a key named "join", and the fields' names that are involved in join are added as BSON document-items. 
	 */

	if (G_UNLIKELY(!j_bson_append_document_begin(&selector->bson, "join", &bson, error)))
	{
		goto _error;
	}

	// Prepare the field name (for selector) by appending namespace and table name to it and then push it to the BSON document instantiated above.
	g_string_append_printf(selector_tablename, "%s_%s.%s", selector->schema->namespace, selector->schema->name, selector_field);
	val.val_string = selector_tablename->str;
	if (G_UNLIKELY(!j_bson_append_value(&bson, "0", J_DB_TYPE_STRING, &val, error)))
	{
		goto _error;
	}

	// Prepare the field name (for sub_selector) by appending namespace and table name to it and then push it to the BSON document instantiated above.
	g_string_append_printf(sub_selector_tablename, "%s_%s.%s", sub_selector->schema->namespace, sub_selector->schema->name, sub_selector_field);
	val.val_string = sub_selector_tablename->str;
	if (G_UNLIKELY(!j_bson_append_value(&bson, "1", J_DB_TYPE_STRING, &val, error)))
	{
		goto _error;
	}

	// Finalize the BSON document.
	if (G_UNLIKELY(!j_bson_append_document_end(&selector->bson, &bson, error)))
	{
		goto _error;
	}

	g_string_free(selector_tablename, TRUE);
	g_string_free(sub_selector_tablename, TRUE);

	return TRUE;

_error:
	g_string_free(selector_tablename, TRUE);
	g_string_free(sub_selector_tablename, TRUE);

	return FALSE;
}
