/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Michael Kuhn
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

#include <stdio.h>

int
main (int argc, char** argv)
{
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JKV) kv = NULL;
	g_autoptr(JObject) object = NULL;

	gchar const* hello_world = "Hello World!";
	guint64 nbytes;

	(void)argc;
	(void)argv;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	object = j_object_new("hello", "world");
	kv = j_kv_new("hello", "world");

	j_object_create(object, batch);
	j_object_write(object, hello_world, strlen(hello_world) + 1, 0, &nbytes, batch);
	j_kv_put(kv, g_strdup(hello_world), strlen(hello_world) + 1, g_free, batch);

	if (j_batch_execute(batch))
	{
		gchar buffer[128];
		g_autofree gpointer value = NULL;
		guint32 length;

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
	}

	return 0;
}
