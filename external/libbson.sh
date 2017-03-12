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

PREFIX="${PWD}/libbson"
VERSION='1.6.1'

if test -d "${PREFIX}"
then
	echo "Error: ${PREFIX} exists already." >&2
	exit 1
fi

TEMP=$(mktemp -d --tmpdir="${PWD}")

trap "rm -rf ${TEMP}" HUP INT TERM 0
cd "${TEMP}"

wget -O "libbson-${VERSION}.tar.gz" "https://github.com/mongodb/libbson/releases/download/${VERSION}/libbson-${VERSION}.tar.gz"
tar xf "libbson-${VERSION}.tar.gz"

cd "libbson-${VERSION}"

autoreconf --force --install --verbose
./configure --prefix="${PREFIX}"
make --jobs="$(nproc)"
make install

rm -rf "${TEMP}"
