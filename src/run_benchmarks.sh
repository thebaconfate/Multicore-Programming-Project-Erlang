#!/usr/bin/env bash

# set -e makes the script exit if any command fails
# set -u makes the script exit if any unset variable is used
# set -o pipefail makes the script exit if any command in a pipeline fails
set -euo pipefail

for i in {1..64}
do
    echo "---"
    echo "> readwrite, central, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_readwrite -s init stop > "benchmarks/central-rw$i.txt"

    echo "---"
    echo "> readwrite multi-server, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_multi_decentral_cached_readwrite -s init stop > "benchmarks/multi-shard-cache-rw$i.txt"

    echo "---"
    echo "> readonly, central, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_readonly -s init stop > "benchmarks/central-r$i.txt"

    echo "---"
    echo "> readonly, multi-server, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_multi_decentral_cached_readonly -s init stop > "benchmarks/multi-shard-cache-r$i.txt"

    echo "---"
    echo "> mixed load, central, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_mixedload -s init stop > "benchmarks/central-mix$i.txt"

    echo "---"
    echo "> mixed load, multi-server, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_multi_decentral_cached_mixedload -s init stop > "benchmarks/multi-shard-cache-mix$i.txt"
done
