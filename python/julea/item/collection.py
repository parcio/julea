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

JULEA_ITEM.j_collection_create.restype = my_void_p


class JCollection:
    def __init__(self, name, batch, get):
        """Initializes and creates a collection

        Args:
            name: The name of the collection
            batch: A batch
            get: A boolean, False when the collection should be created,
                 True if get should be called
        """
        enc_name = name.encode('utf-8')
        if get:
            self.collection = my_void_p
            JULEA_ITEM.j_collection_get(
                ctypes.byref(self.collection), enc_name, batch.get_pointer())
        else:
            self.collection = JULEA_ITEM.j_collection_create(
                enc_name, batch.get_pointer())

    def __del__(self):
        """Dereferences a collection when it is deleted in Python"""
        JULEA_ITEM.j_collection_unref(self.collection)

    def delete(self, batch):
        """Deletes a collection

        Args:
            batch: A batch
        """
        JULEA_ITEM.j_collection_delete(self.collection, batch.get_pointer())

    def get_pointer(self):
        """Gives a pointer to a collection

        Returns:
            collection: A pointer to the collection
        """
        return self.collection
