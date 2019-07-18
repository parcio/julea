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

static gboolean mockup_should_fail;
static bson_t const* mockup_schema;

static gboolean
j_db_internal_schema_create(gchar const* namespace, gchar const* name, bson_t const* schema, JBatch* batch, GError** error)
{
	mockup_schema = schema;
	j_goto_error_frontend(mockup_should_fail, JULEA_FRONTEND_ERROR_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_db_internal_schema_get(gchar const* namespace, gchar const* name, bson_t* schema, JBatch* batch, GError** error)
{
	bson_copy_to(mockup_schema, schema);
	j_goto_error_frontend(mockup_should_fail, JULEA_FRONTEND_ERROR_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_db_internal_schema_delete(gchar const* namespace, gchar const* name, JBatch* batch, GError** error)
{
	j_goto_error_frontend(mockup_should_fail, JULEA_FRONTEND_ERROR_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_db_internal_insert(gchar const* namespace, gchar const* name, bson_t const* metadata, JBatch* batch, GError** error)
{
	j_goto_error_frontend(mockup_should_fail, JULEA_FRONTEND_ERROR_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_db_internal_update(gchar const* namespace, gchar const* name, bson_t const* selector, bson_t const* metadata, JBatch* batch, GError** error)
{
	j_goto_error_frontend(mockup_should_fail, JULEA_FRONTEND_ERROR_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_db_internal_delete(gchar const* namespace, gchar const* name, bson_t const* selector, JBatch* batch, GError** error)
{
	j_goto_error_frontend(mockup_should_fail, JULEA_FRONTEND_ERROR_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_db_internal_query(gchar const* namespace, gchar const* name, bson_t const* selector, gpointer* iterator, JBatch* batch, GError** error)
{
	j_goto_error_frontend(mockup_should_fail, JULEA_FRONTEND_ERROR_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
static gboolean
j_db_internal_iterate(gpointer iterator, bson_t* metadata, GError** error)
{
	j_goto_error_frontend(mockup_should_fail, JULEA_FRONTEND_ERROR_FAILED, "");
	return TRUE;
_error:
	return FALSE;
}
