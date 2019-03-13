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
from .. import JULEA
from .common import my_void_p

(J_SEMANTICS_TEMPLATE_DEFAULT, J_SEMANTICS_TEMPLATE_POSIX,
 J_SEMANTICS_TEMPLATE_TEMPORARY_LOCAL) = (0, 1, 2)

JULEA.j_batch_new_for_template.restype = my_void_p


class JBatch:
    def __init__(self, template):
        """Initializes a batch

        Args:
            template: The template defined in JSemanticsTemplate (C Interface)
        """
        self.batch = JULEA.j_batch_new_for_template(template)

    def __del__(self):
        """Dereferences a batch when it is deleted in Python"""
        JULEA.j_batch_unref(self.batch)

    def __enter__(self):
        """Returns itself when a with-statement is entered

        Returns:
            self: The batch itself
        """
        return self

    def __exit__(self, type, value, traceback):
        """Executes the Batch when the with-statement is left"""
        self.execute()

    def execute(self):
        """Executes the Batch"""
        ret = JULEA.j_batch_execute(self.batch)
        if ret == 1:
            return True
        else:
            return False

    def get_pointer(self):
        """Returns the C Pointer to the batch"""
        return self.batch
