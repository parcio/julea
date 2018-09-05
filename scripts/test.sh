#!/bin/bash

# JULEA - Flexible storage framework
# Copyright (C) 2017-2018 Michael Kuhn
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

set_glib_options
set_path
set_library_path

run_test ()
{
	local build_dir

	# julea-test does not get installed, so we have to find it in the build directory.
	build_dir="$(get_directory "${SELF_DIR}/../build")"

	# FIXME should use functions instead of setup.sh
	setup.sh start
	gtester --keep-going --verbose "$@" "${build_dir}/test/julea-test" || true
	setup.sh stop
}

SPACK_DIR="$(get_directory "${SELF_DIR}/..")/dependencies"

spack_load_dependencies

run_test "$@"
