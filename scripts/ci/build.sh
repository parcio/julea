#!/bin/bash

# JULEA - Flexible storage framework
# Copyright (C) 2024 Michael Kuhn
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

MODE="$1"

. scripts/environment.sh

ls
spack env status
printf "\n\n"
ls .spack-env/
printf "\n\n"

case "${MODE}" in
	release)
		# shellcheck disable=SC2086
		meson setup --prefix="${GITHUB_WORKSPACE}/julea-install" --buildtype=release --werror bld
		ninja -C bld
		ninja -C bld install
		;;
	debug)
		CLANG_LUNDEF=''
		if test "${CC}" = 'clang'
		then
			CLANG_LUNDEF='-Db_lundef=false'
		fi
		# shellcheck disable=SC2086
		meson setup -Db_sanitize=address,undefined ${CLANG_LUNDEF} bld
		ninja -C bld
		;;
	coverage)
		# shellcheck disable=SC2086
		meson setup -Db_coverage=true -Db_sanitize=address,undefined bld
		ninja -C bld
		;;
	*)
		exit 1
		;;
esac
