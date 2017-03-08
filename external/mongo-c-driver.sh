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

PREFIX="${PWD}/mongo-c-driver"
VERSION='1.6.1'

if [ -d "${PREFIX}" ]
then
	echo "Error: ${PREFIX} exists already." >&2
	exit 1
fi

TEMP=$(mktemp -d --tmpdir="${PWD}")

trap "rm -rf ${TEMP}" HUP INT TERM 0
cd "${TEMP}"

wget -O "mongo-c-driver-${VERSION}.tar.gz" "https://github.com/mongodb/mongo-c-driver/releases/download/${VERSION}/mongo-c-driver-${VERSION}.tar.gz"
tar xf "mongo-c-driver-${VERSION}.tar.gz"

cd "mongo-c-driver-${VERSION}"

./configure --prefix="${PREFIX}" --disable-automatic-init-and-cleanup
make --jobs="$(nproc)"
make install

rm -rf "${TEMP}"
