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


# Workaround so Python doesn't convert the pointer to a Python datatype
class my_void_p(ctypes.c_void_p):
    pass


def format_benchmark(input):
    name = input[0] + ':'
    ops = round(input[2] / input[1])
    if len(input) == 3:
        output = "{name: <{width}}{time} seconds ({ops}/s)"
        return output.format(name=name, width=51, time=round(input[1], 3),
                             ops=ops)
    elif len(input) == 4:
        rate = input[3] / input[1]
        unit = "B"
        if rate >= 1024:
            rate = rate / 1024
            unit = "KB"
        if rate >= 1024:
            rate = rate / 1024
            unit = "MB"
        if rate >= 1024:
            rate = rate / 1024
            unit = "GB"
        if rate >= 1024:
            rate = rate / 1024
            unit = "TB"
        rate = round(rate, 1)
        output = "{name: <{width}}{time} seconds ({ops}/s) ({rate} {unit}/s)"
        return output.format(name=name, width=51, time=round(input[1], 3),
                             ops=ops, rate=rate, unit=unit)


def print_test(name):
    print("  {name: <{width}}".format(name=name, width=69), end='', flush=True)
