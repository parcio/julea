/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019-2020 Michael Kuhn
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
#include <julea-object.h>
#include <julea-kv.h>
#include <julea-db.h>

#include <locale.h>
#include <stdio.h>

int
main(int argc, char** argv)
{
	printf("Start with examples...");
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JDBEntry) entry = NULL;
	g_autoptr(JDBSchema) schema = NULL;
	g_autoptr(JKV) kv = NULL;
	g_autoptr(JObject) object = NULL;

	gchar const* hello_world = "Hello World!";
	guint64 nbytes;

	(void)argc;
	(void)argv;

	// Explicitly enable UTF-8 since functions such as g_format_size might return UTF-8 characters.
	setlocale(LC_ALL, "C.UTF-8");

	// FIXME: this example does not clean up after itself and will only work if the object, kv pair and db entry do not exist already
	// FIXME: add more error checking

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	object = j_object_new("hello", "world");
	kv = j_kv_new("hello", "world");
	schema = j_db_schema_new("hello", "world", NULL);
	j_db_schema_add_field(schema, "hello", J_DB_TYPE_STRING, NULL);
	entry = j_db_entry_new(schema, NULL);
	j_db_entry_set_field(entry, "hello", hello_world, strlen(hello_world) + 1, NULL);

	j_object_create(object, batch);
	j_object_write(object, hello_world, strlen(hello_world) + 1, 0, &nbytes, batch);
	j_kv_put(kv, g_strdup(hello_world), strlen(hello_world) + 1, g_free, batch);
	j_db_schema_create(schema, batch, NULL);
	j_db_entry_insert(entry, batch, NULL);

	if (j_batch_execute(batch))
	{
		g_autoptr(JDBIterator) iterator = NULL;
		g_autoptr(JDBSelector) selector = NULL;

		gchar buffer[128] = { '\0' };
		g_autofree gpointer value = NULL;
		guint32 length = 0;
		g_autofree gchar* db_field = NULL;
		guint64 db_length = 0;

		j_object_read(object, buffer, 128, 0, &nbytes, batch);

		if (j_batch_execute(batch))
		{
			printf("Object contains: %s (%" G_GUINT64_FORMAT " bytes)\n", buffer, nbytes);
		}

		j_kv_get(kv, &value, &length, batch);

		if (j_batch_execute(batch))
		{
			printf("KV contains: %s (%" G_GUINT32_FORMAT " bytes)\n", (gchar*)value, length);
		}

		selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
		j_db_selector_add_field(selector, "hello", J_DB_SELECTOR_OPERATOR_EQ, hello_world, strlen(hello_world) + 1, NULL);
		iterator = j_db_iterator_new(schema, selector, NULL);

		while (j_db_iterator_next(iterator, NULL))
		{
			JDBType type;

			j_db_iterator_get_field(iterator, "hello", &type, (gpointer*)&db_field, &db_length, NULL);
			printf("DB contains: %s (%" G_GUINT64_FORMAT " bytes)\n", db_field, db_length);
		}
	}
	printf("Done with example.");
	return 0;
}
