/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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
#include <julea-item.h>

#include <jbatch-internal.h>

#include "test.h"

static gint test_batch_flag;

static
void
on_operation_completed (JBatch* batch, gboolean ret, gpointer user_data)
{
	(void)batch;
	(void)ret;
	(void)user_data;

	g_atomic_int_set(&test_batch_flag, 1);
}

static
void
test_batch_new_free (void)
{
	g_autoptr(JBatch) batch = NULL;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	g_assert(batch != NULL);
}

static
void
test_batch_semantics (void)
{
	JBatch* batch;
	g_autoptr(JSemantics) semantics = NULL;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	g_assert(j_batch_get_semantics(batch) != NULL);

	j_batch_unref(batch);

	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	batch = j_batch_new(semantics);

	g_assert(j_batch_get_semantics(batch) == semantics);

	j_batch_unref(batch);
}

static
void
_test_batch_execute (gboolean async)
{
	g_autoptr(JCollection) collection = NULL;
	g_autoptr(JItem) item = NULL;
	g_autoptr(JBatch) batch = NULL;
	gboolean ret;

	if (async)
	{
		g_atomic_int_set(&test_batch_flag, 0);
	}

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	collection = j_collection_create("test", batch);
	item = j_item_create(collection, "item", NULL, batch);
	j_item_delete(item, batch);
	j_collection_delete(collection, batch);

	if (async)
	{
		j_batch_execute_async(batch, on_operation_completed, NULL);
	}
	else
	{
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}

	if (async)
	{
		while (g_atomic_int_get(&test_batch_flag) != 1)
		{
			g_usleep(1000);
		}
	}
}

static
void
test_batch_execute (void)
{
	_test_batch_execute(FALSE);
}

static
void
test_batch_execute_async (void)
{
	_test_batch_execute(TRUE);
}

void
test_batch (void)
{
	g_test_add_func("/batch/new_free", test_batch_new_free);
	g_test_add_func("/batch/semantics", test_batch_semantics);
	g_test_add_func("/batch/execute", test_batch_execute);
	g_test_add_func("/batch/execute_async", test_batch_execute_async);
}
