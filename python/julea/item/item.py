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
from .. import JULEA_ITEM
from ..lib.common import my_void_p

JULEA_ITEM.j_item_create.restype = my_void_p
JULEA_ITEM.j_item_get_size.restype = ctypes.c_ulonglong
JULEA_ITEM.j_item_get_modification_time.restype = ctypes.c_longlong

JULEA_ITEM.j_item_get.argtypes = [my_void_p, ctypes.POINTER(my_void_p),
                                  ctypes.c_char_p, my_void_p]


class JItem:
    def __init__(self, collection, name, batch, get, distribution=None):
        """Initializes and creates an item

        Args:
            collection: The collection of the item
            name: The name of the item
            batch: A batch
            get: A boolean, False when the item should be created,
                 True if get should be called
            distribution: The distribution of the item(only for create)
        """
        enc_name = name.encode('utf-8')
        if get:
            self.item = my_void_p(0)
            JULEA_ITEM.j_item_get(collection.get_pointer(),
                                  ctypes.byref(self.item), enc_name,
                                  batch.get_pointer())
        else:
            if distribution is None:
                self.item = JULEA_ITEM.j_item_create(
                    collection.get_pointer(), enc_name,
                    distribution, batch.get_pointer())
            else:
                self.item = JULEA_ITEM.j_item_create(
                    collection.get_pointer(), enc_name,
                    distribution.get_pointer(), batch.get_pointer())

    def __del__(self):
        """Dereferences an item when it is deleted in Python"""
        JULEA_ITEM.j_item_unref(self.item)

    def delete(self, batch):
        """Deletes an item from the server

        Args:
            batch: A batch
        """
        JULEA_ITEM.j_item_delete(self.item, batch.get_pointer())

    def status(self, batch):
        """Gets the status of an item

        Args:
            batch: A batch
        """
        JULEA_ITEM.j_item_get_status(self.item, batch.get_pointer())

    def get_size(self):
        """Gets the size of an item
        Requires status to be called earlier

        Returns:
            size: The size of the item
        """
        size = JULEA_ITEM.j_item_get_size(self.item)
        return size

    def get_modification_time(self):
        """Gets the modification time of an item
        Requires status to be called earlier

        Returns:
            modtime: The size of the item
        """
        modtime = JULEA_ITEM.j_item_get_modification_time(self.item)
        return modtime

    def write(self, value, offset, batch):
        """Writes an item

        Args:
            value: bytes holding the data to write
            offset: The offset where the data is written
            batch: A batch
        Returns:
            bytes_written: A c_ulonglong with the bytes written
                           (Access with bytes_written.value)
        """
        length = ctypes.c_ulonglong(len(value))
        value2 = ctypes.c_char_p(value)
        bytes_written = ctypes.c_ulonglong(0)
        JULEA_ITEM.j_item_write(
            self.item, value2, length, offset,
            ctypes.byref(bytes_written), batch.get_pointer())
        return bytes_written

    def read(self, length, offset, batch):
        """Reads an item

        Args:
            length: The length of the data to be read
            offset: The offset where the data is to be read
            batch: A batch
        Returns:
            value: The ctype with the data that was read
                   (Access data with value.value)
            bytes_read: A c_ulonglong with the bytes read
                        (Access with bytes_read.value)
        """
        value = ctypes.c_char_p(bytes(length))
        bytes_read = ctypes.c_ulonglong(0)
        JULEA_ITEM.j_item_read(
            self.item, value, length, offset,
            ctypes.byref(bytes_read), batch.get_pointer())
        return value, bytes_read
