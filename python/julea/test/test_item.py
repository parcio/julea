# JULEA - Flexible storage framework
# Copyright (C) 2019 Johannes Coym
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
from ..lib.common import print_test


def test_create_delete():
    print_test("/item/item/create_delete")
    batch = JBatch(J_SEMANTICS_TEMPLATE_DEFAULT)
    d = JDistribution(J_DISTRIBUTION_ROUND_ROBIN)
    c = JCollection("test-collection", batch, False)
    i = JItem(c, "test-item", batch, False, d)
    batch.execute()

    i.delete(batch)
    batch.execute()
    del i
    print("OK")


def test_read_write():
    print_test("/item/item/read_write")
    batch = JBatch(J_SEMANTICS_TEMPLATE_DEFAULT)
    d = JDistribution(J_DISTRIBUTION_ROUND_ROBIN)
    c = JCollection("test-collection", batch, False)
    i = JItem(c, "test-item", batch, False, d)
    batch.execute()

    bw = i.write("dummy".encode('utf-8'), 0, batch)
    batch.execute()
    assert bw.value == 5

    out = i.read(5, 0, batch)
    batch.execute()
    assert out[0].raw.decode('utf-8') == "dummy"
    assert out[1].value == 5

    i.delete(batch)
    batch.execute()
    del i
    print("OK")


def test_status():
    print_test("/item/item/status")
    batch = JBatch(J_SEMANTICS_TEMPLATE_DEFAULT)
    d = JDistribution(J_DISTRIBUTION_ROUND_ROBIN)
    c = JCollection("test-collection", batch, False)
    i = JItem(c, "test-item", batch, False, d)
    batch.execute()

    dummy = bytearray(42)
    i.write(dummy, 0, batch)
    batch.execute()

    i.status(batch)
    batch.execute()
    size = i.get_size()
    modtime = i.get_modification_time()

    assert modtime != 0
    assert size == 42

    i.delete(batch)
    batch.execute()
    del i
    print("OK")


def item_test():
    test_create_delete()
    test_read_write()
    test_status()
