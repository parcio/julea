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

import ctypes
from .. import JULEA_KV
from ..lib.common import my_void_p

JULEA_KV.j_kv_new.restype = my_void_p


class JKV:

    def __init__(self, namespace, key):
        """Initializes the Key-Value pair and
        gets the new KV pair from the C Interface

        Args:
            namespace: The namespace of the KV pair
            key: The name of the KV pair
        """
        enc_namespace = namespace.encode('utf-8')
        enc_key = key.encode('utf-8')
        self.kv = JULEA_KV.j_kv_new(enc_namespace, enc_key)

    def __del__(self):
        """Dereferences a KV pair when it is deleted in Python"""
        JULEA_KV.j_kv_unref(self.kv)

    def delete(self, batch):
        """Deletes a Key-Value pair from the server

        Args:
            batch: A batch
        """
        JULEA_KV.j_kv_delete(self.kv, batch.get_pointer())

    def put(self, value, batch):
        """Writes a Key-Value pair

        Args:
            value: bytes of data to be written in the KV pair
            batch: A batch
        """
        length = ctypes.c_ulong(len(value))
        value2 = ctypes.c_char_p(value)
        JULEA_KV.j_kv_put(self.kv, value2,
                          length, None, batch.get_pointer())

    def get(self, batch):
        """Reads a Key-Value pair

        Args:
            batch: A batch
        Returns:
            value: The ctype with the data that was read
                   (Access data with value.value[:length.value])
            length: The length of the data
                    (Access with length.value)
        """
        value = ctypes.c_char_p()
        length = ctypes.c_ulong(0)
        JULEA_KV.j_kv_get(self.kv, ctypes.byref(value),
                          ctypes.byref(length), batch.get_pointer())
        return value, length
