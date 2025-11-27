#!/bin/bash

# JULEA - Flexible storage framework
# Copyright (C) 2017-2024 Michael Kuhn
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

SELF_PATH="$(readlink --canonicalize-existing -- "$0")"
SELF_DIR="${SELF_PATH%/*}"
SELF_BASE="${SELF_PATH##*/}"

# shellcheck source=scripts/common
. "${SELF_DIR}/common"

usage ()
{
	echo "Usage: ${SELF_BASE} [arguments]"
	exit 1
}

set_path
set_library_path
set_backend_path
set_hdf_path

run_benchmark ()
{
	local ret

	ret=0

	julea-benchmark "$@" || ret=$?

	return ${ret}
}

if test -z "${JULEA_SPACK_DIR}"
then
	JULEA_SPACK_DIR="$(get_directory "${SELF_DIR}/..")/dependencies"
fi

./bin/spack env activate .
./bin/spack install

run_benchmark "$@"
