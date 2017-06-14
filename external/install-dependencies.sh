#!/bin/sh

# JULEA - Flexible storage framework
# Copyright (C) 2017 Michael Kuhn
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

install_dependency ()
{
	pkg_config="$1"
	spack_pkg="$2"

	test -n "{pkg_config}" || return 1
	test -n "{spack_pkg}" || return 1

	if ! pkg-config --exists "${pkg_config}"
	then
		./bin/spack install "${spack_pkg}"
		install_dir="$(./bin/spack location --install-dir "${spack_pkg}")"
		rm --force "../${spack_pkg}"
		ln --symbolic "${install_dir}" "../${spack_pkg}"
	fi
}

if test ! -d spack
then
	git clone https://github.com/LLNL/spack.git
fi

cd spack

install_dependency glib-2.0 glib
install_dependency leveldb leveldb
install_dependency libbson-1.0 libbson
install_dependency libmongoc-1.0 libmongoc
# FIXME otf
