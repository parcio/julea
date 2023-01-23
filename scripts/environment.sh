# JULEA - Flexible storage framework
# Copyright (C) 2017-2022 Michael Kuhn
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
# shellcheck disable=SC2169,SC3028,SC3054
test -n "${BASH_VERSION}" && SELF_ZERO="${BASH_SOURCE[0]}"

SELF_PATH="$(readlink --canonicalize-existing -- "${SELF_ZERO}")"
SELF_DIR="${SELF_PATH%/*}"
# shellcheck disable=SC2034
SELF_BASE="${SELF_PATH##*/}"

# shellcheck source=scripts/common
. "${SELF_DIR}/common"
# shellcheck source=scripts/spack
. "${SELF_DIR}/spack"

JULEA_ENVIRONMENT_SOURCED=1

# See https://stackoverflow.com/a/28776166
if test -n "${BASH_VERSION}"
then
	(return 0 2>/dev/null) || JULEA_ENVIRONMENT_SOURCED=0
elif test -n "${ZSH_EVAL_CONTEXT}"
then
	case "${ZSH_EVAL_CONTEXT}" in
		*:file)
			;;
		*)
			JULEA_ENVIRONMENT_SOURCED=0
			;;
	esac
fi

if test "${JULEA_ENVIRONMENT_SOURCED}" -eq 0
then
	printf 'Warning: This script should be sourced using ". %s", otherwise changes to the environment will not persist.\n' "${SELF_PATH}" >&2
fi

JULEA_ENVIRONMENT=1

set_path
set_python_path
set_library_path
set_pkg_config_path
set_backend_path
set_hdf_path

SPACK_DIR="$(get_directory "${SELF_DIR}/..")/dependencies"

spack_load_dependencies

# Do not filter out paths contained in CPATH and LIBRARY_PATH.
PKG_CONFIG_ALLOW_SYSTEM_CFLAGS=1
PKG_CONFIG_ALLOW_SYSTEM_LIBS=1

export PKG_CONFIG_ALLOW_SYSTEM_CFLAGS
export PKG_CONFIG_ALLOW_SYSTEM_LIBS

# FIXME The Spack pkg-config does not search in global directories and Meson does not provide a way to override this
PKG_CONFIG_PATH="${PKG_CONFIG_PATH}:/usr/lib64/pkgconfig:/usr/lib/x86_64-linux-gnu/pkgconfig:/usr/lib/pkgconfig:/usr/share/pkgconfig"

export PKG_CONFIG_PATH
