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

#include <julea.h>
#include <julea-db.h>

void
schema_create(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* idx_file[] = {
		"file", NULL
	};
	gchar const* idx_name[] = {
		"name", NULL
	};
	gchar const* idx_min[] = {
		"min", NULL
	};
	gchar const* idx_max[] = {
		"max", NULL
	};

	schema = j_db_schema_new("adios2", "variables", NULL);
	j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
	j_db_schema_add_field(schema, "name", J_DB_TYPE_STRING, NULL);
	j_db_schema_add_field(schema, "dimensions", J_DB_TYPE_UINT64, NULL);
	j_db_schema_add_field(schema, "min", J_DB_TYPE_FLOAT64, NULL);
	j_db_schema_add_field(schema, "max", J_DB_TYPE_FLOAT64, NULL);

	j_db_schema_add_index(schema, idx_file, NULL);
	j_db_schema_add_index(schema, idx_name, NULL);
	j_db_schema_add_index(schema, idx_min, NULL);
	j_db_schema_add_index(schema, idx_max, NULL);

	j_db_schema_create(schema, batch, NULL);
	j_batch_execute(batch);
}

void
entry_insert(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* file = "demo.bp";
	gchar const* name = "temperature";
	guint64 dim = 4;
	gdouble min = 1.0;
	gdouble max = 42.0;

	schema = j_db_schema_new("adios2", "variables", NULL);
	j_db_schema_get(schema, batch, NULL);
	j_batch_execute(batch);

	entry = j_db_entry_new(schema, NULL);
	j_db_entry_set_field(entry, "file", file, strlen(file), NULL);
	j_db_entry_set_field(entry, "name", name, strlen(name), NULL);
	j_db_entry_set_field(entry, "dimensions", &dim, sizeof(dim), NULL);
	j_db_entry_set_field(entry, "min", &min, sizeof(min), NULL);
	j_db_entry_set_field(entry, "max", &max, sizeof(max), NULL);
	j_db_entry_insert(entry, batch, NULL);
	j_batch_execute(batch);
}

void
iterator_get(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBIterator) iterator = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* file = "demo.bp";
	JDBType type;
	guint64 len;
	g_autofree gchar* name = NULL;
	g_autofree guint64* dim = NULL;
	g_autofree gdouble* min = NULL;
	g_autofree gdouble* max = NULL;

	schema = j_db_schema_new("adios2", "variables", NULL);
	j_db_schema_get(schema, batch, NULL);
	j_batch_execute(batch);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
	j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), NULL);

	iterator = j_db_iterator_new(schema, selector, NULL);

	while (j_db_iterator_next(iterator, NULL))
	{
		j_db_iterator_get_field(iterator, "name", &type, (gpointer*)&name, &len, NULL);
		j_db_iterator_get_field(iterator, "dimensions", &type, (gpointer*)&dim, &len, NULL);
		j_db_iterator_get_field(iterator, "min", &type, (gpointer*)&min, &len, NULL);
		j_db_iterator_get_field(iterator, "max", &type, (gpointer*)&max, &len, NULL);
	}
}

void
entry_update(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* file = "demo.bp";
	gchar const* name = "temperature";
	gdouble min = 2.0;
	gdouble max = 22.0;

	schema = j_db_schema_new("adios2", "variables", NULL);
	j_db_schema_get(schema, batch, NULL);
	j_batch_execute(batch);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
	j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), NULL);
	j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), NULL);

	entry = j_db_entry_new(schema, NULL);
	j_db_entry_set_field(entry, "min", &min, sizeof(min), NULL);
	j_db_entry_set_field(entry, "max", &max, sizeof(max), NULL);
	j_db_entry_update(entry, selector, batch, NULL);
	j_batch_execute(batch);
}

void
entry_delete(void)
{
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JDBSelector) selector = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JBatch) batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	gchar const* file = "demo.bp";
	gchar const* name = "temperature";

	schema = j_db_schema_new("adios2", "variables", NULL);
	j_db_schema_get(schema, batch, NULL);
	j_batch_execute(batch);

	selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
	j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ, file, strlen(file), NULL);
	j_db_selector_add_field(selector, "name", J_DB_SELECTOR_OPERATOR_EQ, name, strlen(name), NULL);

	entry = j_db_entry_new(schema, NULL);
	j_db_entry_delete(entry, selector, batch, NULL);
	j_batch_execute(batch);
}

int
main()
{
	schema_create();
	entry_insert();
	iterator_get();
	entry_update();
	entry_delete();
	return 0;
}
