#!/bin/bash

# JULEA - Flexible storage framework
# Copyright (C) 2019-2020 Michael Kuhn
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

usage ()
{
	echo "Usage: ${SELF_BASE} [-f]"
	exit 1
}

check_clang_format ()
{
	local output
	local ret

	if ! command -v clang-format > /dev/null 2>&1
	then
		printf '%s: clang-format is not available.\n' "${SELF_BASE}" >&2
		exit 1
	fi

	ret=0
	output="$(printf 'int main (void) { return 0; }\n' | clang-format --style=file 2>&1)" || ret=$?

	if test ${ret} -ne 0
	then
		printf '%s: clang-format does not work correctly:\n%s\n' "${SELF_BASE}" "${output}" >&2
		exit 1
	fi
}

get_files ()
{
	git ls-files '*.c' '*.h'
}

MODE='diff'

test -n "$1" -a "$1" = '-f' && MODE='format'

check_clang_format

case "${MODE}" in
	diff)
		# Make sure to exit with an error if at least one file differs
		get_files | ( ret=0
		while read file
		do
			diff -u "${file}" <(clang-format --style=file "${file}") || ret=$?
		done
		exit ${ret} ) || exit $?
		;;
	format)
		get_files | while read file
		do
			clang-format -i --style=file "${file}"
		done
		;;
	*)
		usage
		;;
esac
