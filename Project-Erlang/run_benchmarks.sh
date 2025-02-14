#!/usr/bin/env bash

# set -e makes the script exit if any command fails
# set -u makes the script exit if any unset variable is used
# set -o pipefail makes the script exit if any command in a pipeline fails
set -euo pipefail

for i in {1..64}
do
    echo "---"
    echo "> fib, $i threads"
    erl +S $i -noshell -s benchmark test_fib -s init stop > "benchmarks/result-fib-$i.txt"
    echo "---"
    echo "> readwrite, $i threads"
    erl +S $i -noshell -s benchmark test_readwrite -s init stop > "benchmarks/result-readwrite-$i.txt"
    echo "---"
    echo "> readonly, $i threads"
    erl +S $i -noshell -s benchmark test_readonly -s init stop > "benchmarks/result-readonly-$i.txt"
    echo "---"
    echo "> mixed load, $i threads"
    erl +S $i -noshell -s benchmark test_mixedload -s init stop > "benchmarks/result-mixedload-$i.txt"
done
