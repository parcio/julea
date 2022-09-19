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

JDBSelector*
j_db_selector_new(JDBSchema* schema, JDBSelectorMode mode, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	JDBTypeValue val;
	JDBSelector* selector = NULL;

	g_return_val_if_fail(error == NULL || *error == NULL, NULL);
	g_return_val_if_fail(schema != NULL, NULL);

	selector = j_helper_alloc_aligned(128, sizeof(JDBSelector));
	selector->ref_count = 1;
	selector->mode = mode;
	selector->selection_count = 0;
	selector->join_schema = NULL;
	selector->schema = j_db_schema_ref(schema);

	bson_init(&selector->selection);
	bson_init(&selector->joins);
	bson_init(&selector->final);

	selector->join_schema = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	g_hash_table_add(selector->join_schema, g_strdup(schema->name));

	val.val_uint32 = mode;
	if (G_UNLIKELY(!j_bson_append_value(&selector->selection, "m", J_DB_TYPE_UINT32, &val, error)))
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
		// Free schemas.
		j_db_schema_unref(selector->schema);
		g_hash_table_unref(selector->join_schema);
		// BSON document.
		bson_destroy(&selector->selection);
		bson_destroy(&selector->joins);
		bson_destroy(&selector->final);
		// Free Selector.
		g_free(selector);
	}
}

gboolean
j_db_selector_add_field(JDBSelector* selector, gchar const* name, JDBSelectorOperator operator_, gconstpointer value, guint64 length, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_t child;

	JDBType type;
	JDBTypeValue val;
	g_autoptr(GString) key = NULL;

	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	// Current implemntation does not allow more than 500 BSON document elements.
	if (G_UNLIKELY(selector->selection_count + 1 > 500))
	{
		g_set_error_literal(error, J_DB_ERROR, J_DB_ERROR_SELECTOR_TOO_COMPLEX, "selector too complex");
		goto _error;
	}

	// Extract the type of the requested field.
	if (G_UNLIKELY(!j_db_schema_get_field(selector->schema, name, &type, error)))
	{
		goto _error;
	}

	// Prepare key.
	key = g_string_new(name);

	// Initialize BSON document for the field.
	if (G_UNLIKELY(!j_bson_append_document_begin(&selector->selection, key->str, &child, error)))
	{
		goto _error;
	}

	// Add table
	val.val_string = selector->schema->name;
	if (G_UNLIKELY(!j_bson_append_value(&child, "t", J_DB_TYPE_STRING, &val, error)))
	{
		goto _error;
	}

	// Add operator.
	val.val_uint32 = operator_;
	if (G_UNLIKELY(!j_bson_append_value(&child, "o", J_DB_TYPE_UINT32, &val, error)))
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

	// Add value.
	if (G_UNLIKELY(!j_bson_append_value(&child, "v", type, &val, error)))
	{
		goto _error;
	}

	// Finalized the document.
	if (G_UNLIKELY(!j_bson_append_document_end(&selector->selection, &child, error)))
	{
		goto _error;
	}

	selector->selection_count++;

	return TRUE;

_error:
	return FALSE;
}

static gboolean
j_db_selector_add_sub_joins(JDBSelector* selector, JDBSelector* sub_selector)
{
	GHashTableIter iter;
	gpointer key;

	g_hash_table_iter_init(&iter, sub_selector->join_schema);

	while (g_hash_table_iter_next(&iter, &key, NULL))
	{
		// no check here because FALSE could also mean that the key did already exist
		g_hash_table_add(selector->join_schema, g_strdup(key));
	}

	if (!bson_concat(&selector->joins, &sub_selector->joins))
	{
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

static gboolean
j_db_selector_add_sub_selection(JDBSelector* selector, JDBSelector* sub_selector, GError** error)
{
	if (G_UNLIKELY(selector->selection_count + sub_selector->selection_count > 500))
	{
		g_set_error_literal(error, J_DB_ERROR, J_DB_ERROR_SELECTOR_TOO_COMPLEX, "selector too complex");
		goto _error;
	}

	if (sub_selector->selection_count == 0)
	{
		// no need to add an empty selection
		// for example, that is the case for simple join queries of the form 'SELECT * FROM t1, t2 WHERE t1.field1 = t2.field2'
		return TRUE;
	}

	// append the sub selector's selection logic
	if (G_UNLIKELY(!j_bson_append_document(&selector->selection, "_s", &sub_selector->selection, error)))
	{
		goto _error;
	}

	selector->selection_count += sub_selector->selection_count;

	return TRUE;

_error:
	return FALSE;
}

gboolean
j_db_selector_add_selector(JDBSelector* selector, JDBSelector* sub_selector, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(sub_selector != NULL, FALSE);
	g_return_val_if_fail(selector != sub_selector, FALSE);
	// if the schemas are different the semantic is not clear anymore (what would be the joining field?)
	g_return_val_if_fail(selector->schema == sub_selector->schema, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	// fetch selections from the sub_selector
	if (!j_db_selector_add_sub_selection(selector, sub_selector, error))
	{
		goto _error;
	}

	// fetch tables and join information from the sub selector
	if (!j_db_selector_add_sub_joins(selector, sub_selector))
	{
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}

gboolean
j_db_selector_add_join(JDBSelector* selector, gchar const* selector_field, JDBSelector* sub_selector, gchar const* sub_selector_field, GError** error)
{
	J_TRACE_FUNCTION(NULL);

	bson_t join_entry;
	JDBTypeValue val;

	g_return_val_if_fail(selector != NULL, FALSE);
	g_return_val_if_fail(sub_selector != NULL, FALSE);
	g_return_val_if_fail(selector != sub_selector, FALSE);
	g_return_val_if_fail(selector->schema != sub_selector->schema, TRUE);
	g_return_val_if_fail(selector_field != NULL, FALSE);
	g_return_val_if_fail(sub_selector_field != NULL, FALSE);
	g_return_val_if_fail(error == NULL || *error == NULL, FALSE);

	if (!j_bson_init(&join_entry, error))
	{
		goto _error;
	}

	val.val_string = selector_field;
	if (!j_bson_append_value(&join_entry, selector->schema->name, J_DB_TYPE_STRING, &val, error))
	{
		goto _error;
	}

	val.val_string = sub_selector_field;
	if (!j_bson_append_value(&join_entry, sub_selector->schema->name, J_DB_TYPE_STRING, &val, error))
	{
		goto _error;
	}

	// In favor of simplicity the "keys" in the array do not follow the bson standard (i.e., increasing numbers).
	// If we would adhere to the standard we would need a counter and adding the joins from the sub_selector would not be as easy as concatenating the bsons.
	if (!j_bson_append_document(&selector->joins, "0", &join_entry, error))
	{
		goto _error;
	}

	if (!g_hash_table_add(selector->join_schema, g_strdup(sub_selector->schema->name)))
	{
		goto _error;
	}

	if (!j_db_selector_add_sub_joins(selector, sub_selector))
	{
		goto _error;
	}

	if (!j_db_selector_add_sub_selection(selector, sub_selector, error))
	{
		goto _error;
	}

	return TRUE;

_error:
	return FALSE;
}
