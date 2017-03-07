#!/bin/sh

# JULEA - Flexible storage framework
# Copyright (C) 2013-2017 Michael Kuhn
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

usage ()
{
	echo "Usage: ${0##*/}"
	exit 1
}

BUILD_PATH="${SELF_DIR}/../build"

export PATH="${BUILD_PATH}/test:${SELF_DIR}:${PATH}"
export LD_LIBRARY_PATH="${BUILD_PATH}/lib:${LD_LIBRARY_PATH}"

DIRECTORY="${PWD}/results/test/$(date '+%Y-%m')"

mkdir -p "${DIRECTORY}"

echo "Writing results to: ${DIRECTORY}"
cd "${DIRECTORY}"

trap "setup.sh stop" HUP INT TERM 0

setup.sh start
gtester --keep-going --verbose "${BUILD_PATH}/test/test" "$@" 2>&1 | tee --append "$(date --iso-8601)-$(git describe --always).log"
setup.sh stop
