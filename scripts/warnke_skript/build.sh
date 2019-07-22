#!/bin/bash

# JULEA - Flexible storage framework
# Copyright (C) 2019 Benjamin Warnke
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

cc_prefix=$1
afl_path="afl"
tmp_path="/mnt2/julea"
log_path="log"
rm -rf prefix* build* ~/.config/julea/* log ${tmp_path}/*
./scripts/warnke_skript/kill.sh
./scripts/warnke_skript/format.sh
mkdir -p ${log_path}
function julea_compile(){
	(
		compiler=$1
		flags=$2
		asan=$3
		hdf=$(echo $CMAKE_PREFIX_PATH | sed -e 's/:/\n/g' | grep hdf)
		name=$(echo "${compiler}-${flags}" | sed "s/ /-/g" | sed "s/--/-/g" | sed "s/--/-/g")
		export CC=${cc_prefix}${compiler}
		if [ "${asan}" = "asan" ]; then
			export AFL_USE_ASAN=1
			export ASAN_OPTIONS=abort_on_error=1,symbolize=0
			flags="${flags} --debug"
		name="${name}-asan"
		fi
echo		./waf configure ${flags} --out build-${name} --prefix=prefix-${name} --libdir=prefix-${name} --bindir=prefix-${name} --destdir=prefix-${name} --hdf=${hdf}
		./waf configure ${flags} --out build-${name} --prefix=prefix-${name} --libdir=prefix-${name} --bindir=prefix-${name} --destdir=prefix-${name} --hdf=${hdf}
echo		./waf.sh build -j12
		./waf.sh build -j12
echo		./waf.sh install -j12
		./waf.sh install -j12
		rc=$?; if [[ $rc != 0 ]]; then echo "compile build-${name} failed";exit $rc; fi
echo		lcov --zerocounters -d "build-${name}"
		lcov --zerocounters -d "build-${name}"
echo		mkdir -p ${afl_path}/cov
		mkdir -p ${afl_path}/cov
echo		lcov -c -i -d "build-${name}" -o "${afl_path}/cov/build-${name}.info"
		lcov -c -i -d "build-${name}" -o "${afl_path}/cov/build-${name}.info"
	)
}

julea_compile "afl-gcc" "--coverage" "" > ${log_path}/compile1 2>&1
julea_compile "afl-gcc" "--coverage --debug" "" > ${log_path}/compile2 2>&1
julea_compile "afl-gcc" "" "asan" > ${log_path}/compile3 2>&1
julea_compile "afl-clang-fast" "" "" > ${log_path}/compile6 2>&1
julea_compile "afl-clang-fast" "--debug" "" > ${log_path}/compile7 2>&1

