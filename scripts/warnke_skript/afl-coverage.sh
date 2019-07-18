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

rm -rf afl/cov/coverage.info afl/cov/html*
touch afl/cov/coverage.info

for i in $(ls afl/cov/*.info)
do
	if [ -s "$i" ]
	then
		cp $i afl/cov/coverage.info
		break
	fi
done
for i in $(ls afl/cov/*.info)
do
	if [ -s "$i" ]
	then
		lcov -a afl/cov/coverage.info -a $i -o afl/cov/coverage2.info
		mv afl/cov/coverage2.info afl/cov/coverage.info
	fi
done
for i in $(ls afl/cov)
do
	echo afl/cov/$i
	lcov --capture --directory afl/cov/$i --base-directory afl --output-file afl/cov/$i.info
	genhtml afl/cov/$i.info --output-directory afl/cov/html-$i
	if [ -s "afl/cov/$i.info" ]
	then
		lcov -a afl/cov/coverage.info -a afl/cov/$i.info -o afl/cov/coverage2.info
		mv afl/cov/coverage2.info afl/cov/coverage.info
	fi
	rm afl/cov/$i.info
done
genhtml afl/cov/coverage.info --output-directory afl/cov/html
