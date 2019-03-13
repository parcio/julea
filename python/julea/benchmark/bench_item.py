# JULEA - Flexible storage framework
# Copyright (C) 2019 Johannes Coym
# Copyright (C) 2010-2019 Michael Kuhn
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from ..item import (JItem, JCollection, JBatch, JDistribution,
                    J_SEMANTICS_TEMPLATE_DEFAULT, J_DISTRIBUTION_ROUND_ROBIN)
from ..lib.common import format_benchmark
import time


def bench_create(use_batch):
    if use_batch:
        n = 100000
    else:
        n = 1000

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection = JCollection("benchmark", batch, False)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as delete_batch:
        start = time.time()

        with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
            for i in range(0, n):
                name = "benchmark-" + str(i)
                item = JItem(collection, name, batch, False, None)
                item.delete(delete_batch)
                if not use_batch:
                    batch.execute()

        end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection.delete(batch)

    if use_batch:
        return "/item/item/create-batch", end - start, n
    else:
        return "/item/item/create", end - start, n


def bench_delete(use_batch):
    n = 10000

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection = JCollection("benchmark", batch, False)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            name = "benchmark-" + str(i)
            JItem(collection, name, batch, False, None)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as get_batch:
        with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
            for i in range(0, n):
                name = "benchmark-" + str(i)
                item = JItem(collection, name, get_batch, True)
                get_batch.execute()

                item.delete(batch)
                if not use_batch:
                    batch.execute()

    end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection.delete(batch)

    if use_batch:
        return "/item/item/delete-batch", end - start, n
    else:
        return "/item/item/delete", end - start, n


def bench_delete_batch_without_get():
    n = 10000

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection = JCollection("benchmark", batch, False)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            name = "benchmark-" + str(i)
            item = JItem(collection, name, batch, False, None)

            item.delete(batch)

    end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection.delete(batch)

    return "/item/item/delete-batch-without-get", end - start, n


def bench_status(use_batch):
    n = 1000
    dummy = bytearray(1)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection = JCollection("benchmark", batch, False)
        batch.execute()
        item = JItem(collection, "benchmark", batch, False, None)
        item.write(dummy, 0, batch)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for _ in range(0, n):
            item.status(batch)
            if not use_batch:
                batch.execute()

    end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        item.delete(batch)
        collection.delete(batch)

    if use_batch:
        return "/item/item/status-batch", end - start, n
    else:
        return "/item/item/status", end - start, n


def bench_read(use_batch, block_size):
    n = 25000
    dummy = bytearray(block_size)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection = JCollection("benchmark", batch, False)
        item = JItem(collection, "benchmark", batch, False, None)

        for i in range(0, n):
            item.write(dummy, i * block_size, batch)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            # Unused variable required, otherwise glibc crashes somehow
            out = item.read(block_size,  # pylint: disable=unused-variable
                            i * block_size, batch)
            if not use_batch:
                batch.execute()

    end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        item.delete(batch)
        collection.delete(batch)

    if use_batch:
        name = "/item/item/read-batch"
        return name, end - start, n, n * block_size
    else:
        name = "/item/item/read"
        return name, end - start, n, n * block_size


def bench_write(use_batch, block_size):
    n = 25000
    dummy = bytearray(block_size)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        collection = JCollection("benchmark", batch, False)
        batch.execute()
        item = JItem(collection, "benchmark", batch, False, None)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            item.write(dummy, i * block_size, batch)
            if not use_batch:
                batch.execute()

    end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        item.delete(batch)
        collection.delete(batch)

    if use_batch:
        name = "/item/item/write-batch"
        return name, end - start, n, n * block_size
    else:
        name = "/item/item/write"
        return name, end - start, n, n * block_size


def item_bench():
    create = bench_create(False)
    print(format_benchmark(create))
    create_batch = bench_create(True)
    print(format_benchmark(create_batch))

    delete = bench_delete(False)
    print(format_benchmark(delete))
    delete_batch = bench_delete(True)
    print(format_benchmark(delete_batch))
    delete_batch_without_get = bench_delete_batch_without_get()
    print(format_benchmark(delete_batch_without_get))

    status = bench_status(False)
    print(format_benchmark(status))
    status_batch = bench_status(True)
    print(format_benchmark(status_batch))

    read = bench_read(False, 4 * 1024)
    print(format_benchmark(read))
    read_batch = bench_read(True, 4 * 1024)
    print(format_benchmark(read_batch))

    write = bench_write(False, 4 * 1024)
    print(format_benchmark(write))
    write_batch = bench_write(True, 4 * 1024)
    print(format_benchmark(write_batch))
