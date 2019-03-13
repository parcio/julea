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

import ctypes
from .. import JULEA_OBJECT
from ..lib.common import my_void_p

JULEA_OBJECT.j_object_new.restype = my_void_p


class JObject:

    def __init__(self, namespace, name):
        """Initializes the object and gets the new object from the C Interface

        Args:
            namespace: The namespace of the object
            name: The name of the object
        """
        enc_namespace = namespace.encode('utf-8')
        enc_name = name.encode('utf-8')
        self.object = JULEA_OBJECT.j_object_new(enc_namespace, enc_name)

    def __del__(self):
        """Dereferences an object when it is deleted in Python"""
        JULEA_OBJECT.j_object_unref(self.object)

    def create(self, batch):
        """Creates an object

        Args:
            batch: A batch
        """
        JULEA_OBJECT.j_object_create(self.object, batch.get_pointer())

    def delete(self, batch):
        """Deletes an object from the server

        Args:
            batch: A batch
        """
        JULEA_OBJECT.j_object_delete(self.object, batch.get_pointer())

    def status(self, batch):
        """Gets the status of an object

        Args:
            batch: A batch
        Returns:
            modification_time: The modification time of the object
                               (Access data with modification_time.value)
            size: The size of the object (Access data with size.value)
        """
        modification_time = ctypes.c_longlong(0)
        size = ctypes.c_ulonglong(0)
        JULEA_OBJECT.j_object_status(
            self.object, ctypes.byref(modification_time),
            ctypes.byref(size), batch.get_pointer())
        return modification_time, size

    def write(self, value, offset, batch):
        """Writes an object

        Args:
            value: A bytearray holding the data to write
            offset: The offset where the data is written
            batch: A batch
        Returns:
            bytes_written: A c_ulonglong with the bytes written
                           (Access with bytes_written.value)
        """
        length = ctypes.c_ulonglong(len(value))
        value2 = ctypes.create_string_buffer(length.value)
        value2.raw = value
        bytes_written = ctypes.c_ulonglong(0)
        JULEA_OBJECT.j_object_write(
            self.object, ctypes.byref(value2), length, offset,
            ctypes.byref(bytes_written), batch.get_pointer())
        return bytes_written

    def read(self, length, offset, batch):
        """Reads an object

        Args:
            length: The length of the data to be read
            offset: The offset where the data is to be read
            batch: A batch
        Returns:
            value: The ctype with the data that was read
                   (Access data with value.raw)
            bytes_read: A c_ulonglong with the bytes read
                        (Access with bytes_read.value)
        """
        value = ctypes.create_string_buffer(length)
        bytes_read = ctypes.c_ulonglong(0)
        JULEA_OBJECT.j_object_read(
            self.object, ctypes.byref(value), length, offset,
            ctypes.byref(bytes_read), batch.get_pointer())
        return value, bytes_read
