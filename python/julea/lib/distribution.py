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
from .. import JULEA
from .common import my_void_p

(J_DISTRIBUTION_ROUND_ROBIN, J_DISTRIBUTION_SINGLE_SERVER,
 J_DISTRIBUTION_WEIGHTED) = (0, 1, 2)

JULEA.j_distribution_new.restype = my_void_p
JULEA.j_distribution_new_from_bson.restype = my_void_p
JULEA.j_distribution_serialize.restype = my_void_p


class JDistribution:

    def __init__(self, input):
        """Creates a new distribution

        Args:
            input: The type of the distribution or a pointer to a
                   serialized distribution
        """
        if isinstance(input, int):
            self.distribution = JULEA.j_distribution_new(input)
        else:
            self.distribution = JULEA.j_distribution_new_from_bson(input)

    def __del__(self):
        """Dereferences a distribution when it is deleted in Python"""
        JULEA.j_distribution_unref(self.distribution)

    def serialize(self):
        """Serializes the distribution into a BSON.

        Returns:
            b: A pointer to the BSON object in C.
        """
        b = JULEA.j_distribution_serialize(self.distribution)
        return b

    def get_pointer(self):
        """Gets the pointer to the distribution

        Returns:
            distribution: A pointer to the distribution
        """
        return self.distribution
