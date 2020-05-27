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

#ifndef JULEA_TEST_T
#define JULEA_TEST_T

void test_core_background_operation(void);
void test_core_batch(void);
void test_core_cache(void);
void test_core_configuration(void);
void test_core_credentials(void);
void test_core_distribution(void);
void test_core_list(void);
void test_core_list_iterator(void);
void test_core_memory_chunk(void);
void test_core_message(void);
void test_core_semantics(void);

void test_object_distributed_object(void);
void test_object_object(void);

void test_kv_kv(void);
void test_kv_kv_iterator(void);

void test_db_db(void);

void test_item_collection(void);
void test_item_collection_iterator(void);
void test_item_item(void);
void test_item_item_iterator(void);
void test_item_uri(void);

void test_hdf_hdf(void);

#endif
