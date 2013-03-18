#!/bin/bash

cd ..

echo Templates
./tools/setup.sh $PWD start
./build/benchmark/benchmark > benchmark-template-default.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --template posix > benchmark-template-posix.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --template checkpoint > benchmark-template-checkpoint.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --template serial > benchmark-template-serial.log
./tools/setup.sh $PWD stop


echo Atomicity
./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics atomicity=operation > benchmark-atomicity-operation.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics atomicity=none > benchmark-atomicity-none.log
./tools/setup.sh $PWD stop


echo Concurrency
./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics concurrency=overlapping > benchmark-concurrency-overlapping.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics concurrency=non-overlapping > benchmark-concurrency-nonoverlapping.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics concurrency=none > benchmark-concurrency-none.log
./tools/setup.sh $PWD stop


echo Consistency
./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics consistency=immediate > benchmark-consistency-immediate.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics consistency=eventual > benchmark-consistency-eventual.log
./tools/setup.sh $PWD stop


echo Persistency
./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics persistency=immediate > benchmark-persistency-immediate.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics persistency=eventual > benchmark-persistency-eventual.log
./tools/setup.sh $PWD stop


echo Safety
#./tools/setup.sh $PWD start
#./build/benchmark/benchmark --semantics safety=storage > benchmark-safety-storage.log
#./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics safety=network > benchmark-safety-network.log
./tools/setup.sh $PWD stop

./tools/setup.sh $PWD start
./build/benchmark/benchmark --semantics safety=none > benchmark-safety-none.log
./tools/setup.sh $PWD stop

