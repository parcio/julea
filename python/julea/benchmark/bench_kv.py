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

from ..kv import JKV, JBatch, J_SEMANTICS_TEMPLATE_DEFAULT
from ..lib.common import format_benchmark
import time


def bench_put(use_batch):
    n = 200000

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as delete_batch:
        start = time.time()

        with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
            for i in range(0, n):
                name = "benchmark-" + str(i)
                k = JKV("python", name)
                k.put("empty".encode('utf-8'), batch)
                k.delete(delete_batch)
                if not use_batch:
                    batch.execute()

        end = time.time()

    if use_batch:
        return "/kv/kv/put-batch", end - start, n
    else:
        return "/kv/kv/put", end - start, n


def bench_get(use_batch):
    n = 200000

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as delete_batch:
        with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
            for i in range(0, n):
                name = "benchmark-" + str(i)
                k = JKV("python", name)
                k.put(name.encode('utf-8'), batch)
                k.delete(delete_batch)

        start = time.time()

        with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
            for i in range(0, n):
                name = "benchmark-" + str(i)
                k = JKV("python", name)
                k.get(batch)
                if not use_batch:
                    batch.execute()

        end = time.time()

    if use_batch:
        return "/kv/kv/get-batch", end - start, n
    else:
        return "/kv/kv/get", end - start, n


def bench_delete(use_batch):
    n = 200000

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            name = "benchmark-" + str(i)
            k = JKV("python", name)
            k.put("empty".encode('utf-8'), batch)

    start = time.time()

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        for i in range(0, n):
            name = "benchmark-" + str(i)
            k = JKV("python", name)
            k.delete(batch)
            if not use_batch:
                batch.execute()

    end = time.time()

    if use_batch:
        return "/kv/kv/delete-batch", end - start, n
    else:
        return "/kv/kv/delete", end - start, n


def kv_bench():
    create = bench_put(False)
    print(format_benchmark(create))
    create_batch = bench_put(True)
    print(format_benchmark(create_batch))

    read = bench_get(False)
    print(format_benchmark(read))
    read_batch = bench_get(True)
    print(format_benchmark(read_batch))

    delete = bench_delete(False)
    print(format_benchmark(delete))
    delete_batch = bench_delete(True)
    print(format_benchmark(delete_batch))
