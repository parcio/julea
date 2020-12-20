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

from ..kv import JKV, JBatch, J_SEMANTICS_TEMPLATE_DEFAULT
from ..lib.common import print_test


def test_put_delete():
    print_test("/kv/kv/put_delete")
    batch = JBatch(J_SEMANTICS_TEMPLATE_DEFAULT)
    k = JKV("python", "test")
    k.put("dummy".encode('utf-8'), batch)
    batch.execute()

    k.delete(batch)
    batch.execute()
    del k
    print("OK")


def test_put_get():
    print_test("/kv/kv/put_get")
    batch = JBatch(J_SEMANTICS_TEMPLATE_DEFAULT)
    k = JKV("python", "test")
    k.put("dummy".encode('utf-8'), batch)
    batch.execute()

    out = k.get(batch)
    batch.execute()
    assert out[0].value[:out[1].value].decode('utf-8') == "dummy"
    assert out[1].value == 5

    k.delete(batch)
    batch.execute()
    del k
    print("OK")


def kv_test():
    test_put_delete()
    test_put_get()
