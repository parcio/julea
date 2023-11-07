#!/bin/env python3

# JULEA - Flexible storage framework
# Copyright (C) 2023 Timm Leon Erxleben
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

# This is a simple script to generate HDF5 files for tests.

import h5py
import numpy as np

rng = np.random.default_rng()

f = h5py.File('testfile.h5', 'w')

# create some groups
g1 = f.create_group("g1")
g1_1 = g1.create_group("g1_1")
g2 = f.create_group("g2")

# create some datasets with different data types and sizes
f["2D-f64"] = rng.random(size=(10, 8), dtype=np.float64)
g1["3D-f64"] = rng.random(size=(5, 10, 3), dtype=np.float64)
g1_1["1D-f32"] = rng.random(size=(10), dtype=np.float32)
g2["0D-int"] = rng.integers(0, 500, dtype=np.uint32)
g2["4D-int64"] = rng.integers(0, 500, size=(2,2,2,2), dtype=np.int64)
g1["4D-int32"] = rng.integers(0, 500, size=(2,2,2,2), dtype=np.int32)
g2["string-set"] = np.bytes_("I am a fixed size string!")
#2["var-string-set"] = "I am a variable sized string!"

# create some attributes
f.attrs["file-attr"] = 42
#g1.attrs["group1-attr"] = "I am a group attribute and a variable sized string"
g1.attrs["group1-attr-fixed-str"] = np.bytes_("I am a group attribute and a fixed size string")
g2.attrs["group2-attr"] = rng.random(size=(2,2))
g2["string-set"].attrs["dset-attr"] = 3.14159

f.close()

