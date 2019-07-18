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

mkdir -p ${afl_path}/start-files

# this may fail but improves afl speed
sudo echo core >/proc/sys/kernel/core_pattern
sudo echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# variables required because of slurm
first_index=$1
if [ -z "$first_index" ] ; then	first_index=9 ; fi
afl_path="afl"
tmp_path="/mnt2/julea"
log_path="log"
mkdir -p ${log_path}

function julea_run(){
	(
		compiler=$1
		flags=$2
		asan=$3
		index=$4
		aflfuzzflags=$5
		servercount=$6
		programname=$7
		servers="$(hostname)"
		if [ "${servercount}" -eq "0" ]; then
			component="client"
		else
			compiler="afl-gcc"
			component="server"
			i=0
			port=$((10000 + ${index} * 10 + $i))
			servers="$(hostname):${port}"
			for (( i=1; i < ${servercount}; i++ ))
			do
				port=$((10000 + ${index} * 10 + $i))
				servers="${servers},$(hostname):${port}"
			done
		fi
		name=$(echo "${compiler}-${flags}" | sed "s/ /-/g" | sed "s/--/-/g" | sed "s/--/-/g")
		unset AFL_USE_ASAN
		unset ASAN_OPTIONS
		export G_MESSAGES_DEBUG=
		if [ "${asan}" == "asan" ]
		then
			export AFL_USE_ASAN=1
			export ASAN_OPTIONS=abort_on_error=1,symbolize=0
			name="${name}-asan"
		fi
		echo compiler $compiler
		echo flags $flags
		echo asan $asan
		echo index $index
		echo aflfuzzflags $aflfuzzflags
		echo servercount $servercount
		export LD_LIBRARY_PATH="prefix-${name}/lib/:${LD_LIBRARY_PATH}"
		./build-${name}/tools/julea-config --user \
			--object-servers="${servers}" --object-backend=posix --object-component="${component}" --object-path="${tmp_path}/object${index}" \
			--kv-servers="${servers}"     --kv-backend=sqlite    --kv-component="${component}"     --kv-path="${tmp_path}/kv${index}" \
			--db-servers="${servers}"    --db-backend=sqlite   --db-component="${component}"    --db-path=":memory:"
		eval "mv ~/.config/julea/julea ~/.config/julea/julea${index}"
		export G_SLICE=always-malloc
		export G_DEBUG=gc-friendly,resident-modules
		export AFL_NO_UI=1
		export AFL_NO_AFFINITY=1
		export AFL_SKIP_CRASHES=1
		export JULEA_CONFIG=~/.config/julea/julea${index}
		export AFL_DONT_OPTIMIZE=1
		export AFL_HARDEN=1
		export PATH=~/afl:$PATH
		export AFL_SKIP_CPUFREQ=1
		mkdir -p ${afl_path}/out
		for (( i=0; i < ${servercount}; i++ ))
		do
			(
				./build-${name}/tools/julea-config --user \
					--object-servers="${servers}" --object-backend=posix --object-component="${component}" --object-path="${tmp_path}/object${index}-$i" \
					--kv-servers="${servers}"     --kv-backend=sqlite    --kv-component="${component}"     --kv-path="${tmp_path}/kv${index}-$i" \
					--db-servers="${servers}"    --db-backend=sqlite   --db-component="${component}"    --db-path=":memory:"
				eval "mv ~/.config/julea/julea ~/.config/julea/julea${index}-$i"
				export JULEA_CONFIG=~/.config/julea/julea${index}-$i
				echo ./build-${name}/server/julea-server --port=$((10000 + ${index} * 10 + $i))
				     ./build-${name}/server/julea-server --port=$((10000 + ${index} * 10 + $i)) &
			)
		done
		sleep 2s
		echo "export JULEA_CONFIG=~/.config/julea/julea${index}"
		for a in ${afl_path}/start-files/*.bin; do
			echo "cat $a | ./build-${name}/test-afl/${programname}"
			      cat $a | ./build-${name}/test-afl/${programname}
		done
		if [ "${asan}" != "asan" ]
		then
			#asan not first in library list - first ist afl - all asan tests will fail
			if [ "${servercount}" -gt "0" ]
			then
				#some julea-tests assume that component=server
				(
					./build-${name}/test/julea-test
					rc=$?; if [[ $rc != 0 ]]; then echo "julea-test build-${test_name} failed";exit $rc; fi
				)
			fi
		fi
		afl-fuzz ${aflfuzzflags} fuzzer${index} -i ${afl_path}/start-files -o ${afl_path}/out ./build-${name}/test-afl/${programname}
	)
}

cp test-afl/bin/* ${afl_path}/start-files/
c=$(ls -la ${afl_path}/start-files/ | wc -l)
if (( $c < 10 )); then
    i=0
    (
		servers="$(hostname):${port}"
		component="client"
		index=$i
		name="afl-clang-fast-debug"
		./build-${name}/tools/julea-config --user \
			--object-servers="${servers}" --object-backend=posix --object-component="${component}" --object-path="${tmp_path}/object${index}" \
			--kv-servers="${servers}"     --kv-backend=sqlite    --kv-component="${component}"     --kv-path="${tmp_path}/kv${index}" \
			--db-servers="${servers}"    --db-backend=sqlite   --db-component="${component}"    --db-path=":memory:"
                eval "mv ~/.config/julea/julea ~/.config/julea/julea${index}"
		export LD_LIBRARY_PATH=prefix-${name}/lib/:$LD_LIBRARY_PATH
		export JULEA_CONFIG=~/.config/julea/julea${i}
		./build-${name}/test-afl/julea-test-afl-db-backend ${afl_path}
	)
fi
i=${first_index};
i=$(($i + 1)); julea_run "afl-gcc" "" "" "$i" "-m none -t 10000 -S" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s
i=$(($i + 1)); julea_run "afl-gcc" "--debug" "" "$i" "-m none -t 10000 -S" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s
i=$(($i + 1)); julea_run "afl-gcc" "" "asan" "$i" "-m none -t 10000 -S" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s
i=$(($i + 1)); julea_run "afl-gcc" "--testmockup" "" "$i" "-m none -t 10000 -S" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s
i=$(($i + 1)); julea_run "afl-gcc" "--testmockup --debug" "" "$i" "-m none -t 10000 -S" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s
i=$(($i + 1)); julea_run "afl-clang-fast" "" "" "$i" "-m none -t 10000 -M" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s
i=$(($i + 1)); julea_run "afl-clang-fast" "--debug" "" "$i" "-m none -t 10000 -M" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s
i=$(($i + 1)); julea_run "afl-clang-fast" "--testmockup" "" "$i" "-m none -t 10000 -M" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s
i=$(($i + 1)); julea_run "afl-clang-fast" "--testmockup --debug" "" "$i" "-m none -t 10000 -M" "0" "julea-test-afl-db-backend" > "${log_path}/run$i.out" 2>"${log_path}/run$i.err" &
sleep 0.5s


echo "done"
wait
