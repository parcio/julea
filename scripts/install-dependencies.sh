#!/bin/bash

# JULEA - Flexible storage framework
# Copyright (C) 2017-2020 Michael Kuhn
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

. "${SELF_DIR}/common"
. "${SELF_DIR}/spack"

usage ()
{
	echo "Usage: ${SELF_BASE} minimal|standard|full"
	exit 1
}

MODE='standard'

test -n "$1" && MODE="$1"

SPACK_DIR="$(get_directory "${SELF_DIR}/..")/dependencies"

case "${MODE}" in
	minimal|standard|full)
		spack_install_dependencies "${MODE}"
		;;
	*)
		usage
		;;
esac
