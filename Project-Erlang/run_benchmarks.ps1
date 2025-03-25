#!/usr/bin/env pwsh

$ErrorActionPreference = "Stop"
$DirName = "benchmarks"

if (-Not (Test-Path $DirName)) {
    mkdir $DirName | Out-Null
}


for ($i = 1; $i -le 64; $i++) {
    Write-Output "---"
    Write-Output "> readwrite centralized, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_readwrite -s init stop | Out-File -Encoding utf8 "benchmarks/central-rw$i.txt"

    Write-Output "---"
    Write-Output "> readwrite multi-server, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_multi_decentral_cached_readwrite -s init stop | Out-File -Encoding utf8 "benchmarks/multi-shard-cache-rw$i.txt"

    Write-Output "---"
    Write-Output "> readonly centralized, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_readonly -s init stop | Out-File -Encoding utf8 "benchmarks/central-r$i.txt"

    Write-Output "---"
    Write-Output "> readonly multi-server, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_multi_decentral_cached_readonly -s init stop | Out-File -Encoding utf8 "benchmarks/multi-shard-cache-r$i.txt"

    Write-Output "---"
    Write-Output "> mixed load centralized, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_mixedload -s init stop | Out-File -Encoding utf8 "benchmarks/central-mix$i.txt"

    Write-Output "---"
    Write-Output "> mixed load, multi-server, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_multi_decentral_cached_mixedload -s init stop | Out-File -Encoding utf8 "benchmarks/multi-shard-cache-mix$i.txt"
}

