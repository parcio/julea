/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#include <locale.h>

#include <julea.h>

#include "test.h"

int
main(int argc, char** argv)
{
	gint ret;

	// Explicitly enable UTF-8 since functions such as g_format_size might return UTF-8 characters.
	setlocale(LC_ALL, "C.UTF-8");

	g_test_init(&argc, &argv, NULL);

	// Core
	test_core_background_operation();
	test_core_batch();
	test_core_cache();
	test_core_configuration();
	test_core_credentials();
	test_core_distribution();
	test_core_list();
	test_core_list_iterator();
	test_core_memory_chunk();
	test_core_message();
	test_core_semantics();

	// Object client
	test_object_distributed_object();
	test_object_object();

	// KV client
	test_kv_kv();
	test_kv_kv_iterator();

	// DB client
	test_db_db();

	// Item client
	test_item_collection();
	test_item_collection_iterator();
	test_item_item();
	test_item_item_iterator();
	test_item_uri();

	// HDF5 client
	test_hdf_hdf();

	ret = g_test_run();

	return ret;
}
