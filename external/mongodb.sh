#!/bin/sh

# JULEA - Flexible storage framework
# Copyright (C) 2012-2017 Michael Kuhn
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

PREFIX="${PWD}/mongodb"
BASE='mongodb-src-r3.4.2'
FILE="${BASE}.tar.gz"

if [ -d "${PREFIX}" ]
then
	echo "Error: ${PREFIX} exists already." >&2
	exit 1
fi

TEMP=$(mktemp -d --tmpdir="${PWD}")

trap "rm -rf ${TEMP}" HUP INT TERM 0
cd "${TEMP}"

wget "https://fastdl.mongodb.org/src/${FILE}"
tar xf "${FILE}"

cd "${BASE}"

scons --jobs="$(nproc)" all
scons --prefix="${PREFIX}" install

rm -rf "${TEMP}"
