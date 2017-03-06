#!/bin/sh

# JULEA - Flexible storage framework
# Copyright (C) 2013-2017 Michael Kuhn
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

set -e

PREFIX="${PWD}/otf"
VERSION='1.12.5salmon'

if [ -d "${PREFIX}" ]
then
	echo "Error: ${PREFIX} exists already." >&2
	exit 1
fi

TEMP=$(mktemp -d --tmpdir="${PWD}")

trap "rm -rf ${TEMP}" HUP INT TERM 0
cd "${TEMP}"

wget -O "otf-${VERSION}.tar.gz" "http://wwwpub.zih.tu-dresden.de/~mlieber/dcount/dcount.php?package=otf&get=OTF-${VERSION}.tar.gz"
tar xf "otf-${VERSION}.tar.gz"

cd "OTF-${VERSION}"

./configure --prefix="${PREFIX}"
make --jobs=$(nproc)
make install

rm -rf "${TEMP}"
