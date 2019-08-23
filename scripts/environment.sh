# JULEA - Flexible storage framework
# Copyright (C) 2017-2019 Michael Kuhn
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

SELF_ZERO="$0"
test -n "${BASH_VERSION}" && SELF_ZERO="${BASH_SOURCE[0]}"

SELF_PATH="$(readlink --canonicalize-existing -- "${SELF_ZERO}")"
SELF_DIR="${SELF_PATH%/*}"
SELF_BASE="${SELF_PATH##*/}"

. "${SELF_DIR}/common"
. "${SELF_DIR}/spack"

set_path
set_library_path
set_pkg_config_path
set_backend_path

SPACK_DIR="$(get_directory "${SELF_DIR}/..")/dependencies"

spack_load_dependencies
