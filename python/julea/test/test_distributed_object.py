# JULEA - Flexible storage framework
# Copyright (C) 2019-2020 Johannes Coym
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

from ..object import (JDistributedObject, JBatch, JDistribution,
                      J_SEMANTICS_TEMPLATE_DEFAULT, J_DISTRIBUTION_ROUND_ROBIN)
from ..lib.common import print_test


def test_create_delete():
    print_test("/object/distributed-object/create_delete")
    batch = JBatch(J_SEMANTICS_TEMPLATE_DEFAULT)
    d = JDistribution(J_DISTRIBUTION_ROUND_ROBIN)
    o = JDistributedObject("python", "test", d)
    o.create(batch)
    batch.execute()

    o.delete(batch)
    batch.execute()
    del o
    print("OK")


def test_read_write():
    print_test("/object/distributed-object/read_write")
    batch = JBatch(J_SEMANTICS_TEMPLATE_DEFAULT)
    d = JDistribution(J_DISTRIBUTION_ROUND_ROBIN)
    o = JDistributedObject("python", "test", d)
    o.create(batch)
    batch.execute()

    bw = o.write("dummy".encode('utf-8'), 0, batch)
    batch.execute()
    assert bw.value == 5

    out = o.read(5, 0, batch)
    batch.execute()
    assert out[0].value.decode('utf-8') == "dummy"
    assert out[1].value == 5

    o.delete(batch)
    batch.execute()
    del o
    print("OK")


def test_status():
    print_test("/object/distributed-object/status")
    batch = JBatch(J_SEMANTICS_TEMPLATE_DEFAULT)
    d = JDistribution(J_DISTRIBUTION_ROUND_ROBIN)
    o = JDistributedObject("python", "test", d)
    o.create(batch)
    batch.execute()

    dummy = bytes(42)
    o.write(dummy, 0, batch)
    batch.execute()

    modtime, size = o.status(batch)
    batch.execute()

    assert modtime.value != 0
    assert size.value == 42

    o.delete(batch)
    batch.execute()
    del o
    print("OK")


def distributed_object_test():
    test_create_delete()
    test_read_write()
    test_status()
