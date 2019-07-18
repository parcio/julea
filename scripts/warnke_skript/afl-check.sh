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

files=""
files="${files} $(ls afl/out/*/crashes/* | grep -v README )"
files="${files} $(ls afl/start-files/* | grep -v README )"
./scripts/warnke_skript/format.sh
rm -rf /mnt2/julea/* b
mkdir b
(export AFL_USE_ASAN=1; export ASAN_OPTIONS=abort_on_error=1,symbolize=0; ./waf configure --debug --out build-gcc-asan --prefix=prefix-gcc-asan --libdir=prefix-gcc-asan --bindir=prefix-gcc-asan --destdir=prefix-gcc-asan&& ./waf.sh build && ./waf.sh install)
i=300
(export LD_LIBRARY_PATH=prefix-gcc-asan/lib/:$LD_LIBRARY_PATH; ./build-gcc-asan/tools/julea-config --user \
  --object-servers="$(hostname)" --kv-servers="$(hostname)" \
  --db-servers="$(hostname)" \
  --object-backend=posix --object-component=client --object-path="/mnt2/julea/object${i}" \
  --kv-backend=sqlite --kv-component=client --kv-path="/mnt2/julea/kv${i}" \
  --db-backend=sqlite --db-component=client --db-path="/mnt2/julea/db${i}")
mv ~/.config/julea/julea ~/.config/julea/julea${i}

j=0

for f in ${files}
do
for g in gcc-asan
do
echo "using binary : $g"
mkdir b/${g}
	for programname in "julea-test-afl-db-backend" "julea-test-afl-db-schema"
	do

	rm -rf /mnt2/julea/*
	(
		export LD_LIBRARY_PATH=prefix-${g}/lib/:$LD_LIBRARY_PATH
		export JULEA_CONFIG=~/.config/julea/julea${i}
		export ASAN_OPTIONS=fast_unwind_on_malloc=0
		export G_DEBUG=resident-modules,gc-friendly
		export G_MESSAGES_DEBUG=all
		export G_SLICE=always-malloc
		echo ${programname} > x
		cat $f | valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes --error-exitcode=1 --track-origins=yes  \
			--suppressions=./dependencies/opt/spack/linux-ubuntu19.04-x86_64/gcc-8.3.0/glib-2.56.3-y4kalfnkzahoclmqcqcpwvxzw4nepwsi/share/glib-2.0/valgrind/glib.supp \
			./build-${g}/test-afl/${programname} >> x 2>&1)
	r=$?
	if [ $r -eq 0 ]; then
		echo "invalid $f $g"
	else
		echo "valid $f $g"
		cp $f b/${g}/
		exit 1
	fi
	mv x $j-${programname}-$g.tmp-file
done
done
	j=$(($j + 1))
done
