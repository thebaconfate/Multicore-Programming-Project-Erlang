#!/usr/bin/env bash

# set -e makes the script exit if any command fails
# set -u makes the script exit if any unset variable is used
# set -o pipefail makes the script exit if any command in a pipeline fails
set -euo pipefail

DIR="firefly-session-2"

if [ ! -d "$DIR" ]; then
    mkdir "$DIR"
    echo "Directory '$DIR' created."
else
    echo "Directory '$DIR' already exists"
fi

for i in {1..128}
do
    echo "---"
    echo "> readwrite, central, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_readwrite -s init stop > "$DIR/central-rw$i.txt"

    echo "---"
    echo "> readwrite sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_sharded_cached_readwrite -s init stop > "$DIR/sharded-cached-rw$i.txt"

    echo "---"
    echo "> readwrite worker, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_worker_sharded_cached_readwrite -s init stop > "$DIR/worker-sharded-cached-rw$i.txt"

    echo "---"
    echo "> readonly centralized, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_readonly -s init stop > "$DIR/central-r$i.txt"

    echo "---"
    echo "> readonly sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_sharded_cached_readonly -s init stop > "$DIR/sharded-cached-r$i.txt"

    echo "---"
    echo "> readonly worker, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_worker_sharded_cached_readonly -s init stop > "$DIR/worker-sharded-cached-r$i.txt"

    echo "---"
    echo "> mixed load centralized, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_mixedload -s init stop > "$DIR/central-mix$i.txt"

    echo "---"
    echo "> mixed load sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_sharded_cached_mixedload -s init stop > "$DIR/sharded-cached-mix$i.txt"

    echo "---"
    echo "> mixed load, worker, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_worker_sharded_cached_mixedload -s init stop > "$DIR/worker-sharded-cached-mix$i.txt"

done
