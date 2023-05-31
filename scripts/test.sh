#!/bin/bash

# JULEA - Flexible storage framework
# Copyright (C) 2017-2023 Michael Kuhn
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
# shellcheck source=scripts/spack
. "${SELF_DIR}/spack"

usage ()
{
	echo "Usage: ${SELF_BASE} [arguments]"
	exit 1
}

#export G_SLICE=always-malloc
export G_SLICE=debug-blocks

set_path
set_python_path
set_library_path
set_backend_path
set_hdf_path

run_test ()
{
	local build_dir
	local ret

	build_dir="$(get_directory "${SELF_DIR}/..")/bld"
	ret=0

	test -d "${build_dir}" || error_build_directory_not_found

	meson test -C "${build_dir}" --no-rebuild --print-errorlogs --verbose --test-args "$*" || ret=$?

	if test -n "${CI}" -a ${ret} -ne 0
	then
		journalctl --boot GLIB_DOMAIN=JULEA || true
	fi

	return ${ret}
}

if test -z "${JULEA_SPACK_DIR}"
then
	JULEA_SPACK_DIR="$(get_directory "${SELF_DIR}/..")/dependencies"
fi

spack_load_dependencies

run_test "$@"
