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

from julea.object import JObject, JBatch, J_SEMANTICS_TEMPLATE_DEFAULT


def main():
    object = JObject("hello", "world")
    hello_world = "Hello World!"

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        object.create(batch)
        bw = object.write(hello_world.encode('utf-8'), 0, batch)

    with JBatch(J_SEMANTICS_TEMPLATE_DEFAULT) as batch:
        out = object.read(bw.value, 0, batch)

    print("Object contains: {buffer} ({length} bytes)".format(
        buffer=out[0].raw.decode('utf-8'), length=out[1].value))


if __name__ == "__main__":
    main()
