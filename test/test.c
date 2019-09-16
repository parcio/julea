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

#include "test.h"

int
main (int argc, char** argv)
{
	gint ret;

	g_test_init(&argc, &argv, NULL);

	// Core
	test_background_operation();
	test_batch();
	test_cache();
	test_configuration();
	test_distribution();
	test_list();
	test_list_iterator();
	test_memory_chunk();
	test_message();
	test_semantics();

	// Object client
	test_object_distributed_object();
	test_object_object();

	// KV client
	test_kv_iterator();

	// Item client
	test_collection();
	test_collection_iterator();
	test_item();
	test_item_iterator();
	test_uri();

	// DB backend
	test_db_backend();

	// HDF5 client
	test_hdf();

	ret = g_test_run();

	return ret;
}
