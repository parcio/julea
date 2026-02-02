#!/usr/bin/env bash

# JULEA - Flexible storage framework
# Copyright (C) 2019-2024 Michael Kuhn
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
	echo "Usage: ${SELF_BASE}"
	exit 1
}

check_shellcheck ()
{
	if ! command -v shellcheck > /dev/null 2>&1
	then
		printf '%s: shellcheck is not available.\n' "${SELF_BASE}" >&2
		exit 1
	fi
}

get_files ()
{
	git ls-files '*.sh'
}

check_shellcheck

# Make sure to exit with an error if at least one file has problems
get_files | ( ret=0
while read -r file
do
	shell_arg=''

	if test "${file##*/}" = 'environment.sh'
	then
		shell_arg='--shell=dash'
	fi

	shellcheck --external-sources --check-sourced ${shell_arg} "${file}" || ret=$?
done
exit ${ret} ) || exit $?
