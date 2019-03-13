# JULEA - Flexible storage framework
# Copyright (C) 2019 Johannes Coym
# Copyright (C) 2017-2019 Michael Kuhn
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

from ..object import JObject, JBatch, J_SEMANTICS_TEMPLATE_DEFAULT
from ..lib.common import format_benchmark
import time


def bench_create(use_batch):
    n = 100000

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as delete_batch:
        start = time.time()

        with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
            for i in range(0, n):
                name = "benchmark-" + str(i)
                o = JObject("python", name)
                o.create(batch)
                o.delete(delete_batch)
                if not use_batch:
                    batch.execute()

        end = time.time()

    if use_batch:
        return "/object/object/create-batch", end - start, n
    else:
        return "/object/object/create", end - start, n


def bench_delete(use_batch):
    n = 100000

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            name = "benchmark-" + str(i)
            o = JObject("python", name)
            o.create(batch)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            name = "benchmark-" + str(i)
            o = JObject("python", name)
            o.delete(batch)
            if not use_batch:
                batch.execute()

    end = time.time()

    if use_batch:
        return "/object/object/delete-batch", end - start, n
    else:
        return "/object/object/delete", end - start, n


def bench_status(use_batch):
    n = 200000
    dummy = bytearray(1)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        o = JObject("python", "benchmark")
        o.delete(batch)
        o.create(batch)
        o.write(dummy, 0, batch)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for _ in range(0, n):
            o.status(batch)
            if not use_batch:
                batch.execute()

    end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        o.delete(batch)

    if use_batch:
        return "/object/object/status-batch", end - start, n
    else:
        return "/object/object/status", end - start, n


def bench_read(use_batch, block_size):
    n = 200000
    dummy = bytearray(block_size)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        o = JObject("python", "benchmark")
        o.delete(batch)
        o.create(batch)

        for i in range(0, n):
            o.write(dummy, i * block_size, batch)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            o.read(block_size, i * block_size, batch)
            if not use_batch:
                batch.execute()

    end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        o.delete(batch)

    if use_batch:
        return "/object/object/read-batch", end - start, n, n * block_size
    else:
        return "/object/object/read", end - start, n, n * block_size


def bench_write(use_batch, block_size):
    n = 200000
    dummy = bytearray(block_size)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        o = JObject("python", "benchmark")
        o.delete(batch)
        o.create(batch)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            o.write(dummy, i * block_size, batch)
            if not use_batch:
                batch.execute()

    end = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        o.delete(batch)

    if use_batch:
        return "/object/object/write-batch", end - start, n, n * block_size
    else:
        return "/object/object/write", end - start, n, n * block_size


def object_bench():
    create = bench_create(False)
    print(format_benchmark(create))
    create_batch = bench_create(True)
    print(format_benchmark(create_batch))

    delete = bench_delete(False)
    print(format_benchmark(delete))
    delete_batch = bench_delete(True)
    print(format_benchmark(delete_batch))

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
