#dict!/usr/bin/env pwsh

$ErrorActionPreference = "Stop"
$DirName = "benchmarks-omen-maps"

if (-Not (Test-Path $DirName)) {
    mkdir $DirName | Out-Null
}


for ($i = 1; $i -le 64; $i++) {
    Write-Output "---"
    Write-Output "> readwrite centralized, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_readwrite -s init stop | Out-File -Encoding utf8 "$DirName/central-rw$i.txt"

    Write-Output "---"
    Write-Output "> readwrite sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_sharded_cached_readwrite -s init stop | Out-File -Encoding utf8 "$DirName/shard-cached-rw$i.txt"

    Write-Output "---"
    Write-Output "> readwrite worker, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_worker_sharded_cached_readwrite -s init stop | Out-File -Encoding utf8 "$DirName/work-shard-cached-rw$i.txt"

    Write-Output "---"
    Write-Output "> readonly centralized, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_readonly -s init stop | Out-File -Encoding utf8 "$DirName/central-r$i.txt"

    Write-Output "---"
    Write-Output "> readonly sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_sharded_cached_readonly -s init stop | Out-File -Encoding utf8 "$DirName/shard-cached-r$i.txt"

    Write-Output "---"
    Write-Output "> readonly worker, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_worker_sharded_cached_readonly -s init stop | Out-File -Encoding utf8 "$DirName/work-shard-cached-r$i.txt"

    Write-Output "---"
    Write-Output "> mixed load centralized, $i threads"
    erl +S $i -noshell -s benchmark test_centralized_mixedload -s init stop | Out-File -Encoding utf8 "$DirName/central-mix$i.txt"

    Write-Output "---"
    Write-Output "> mixed load sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_sharded_cached_mixedload -s init stop | Out-File -Encoding utf8 "$DirName/shard-cached-mix$i.txt"

    Write-Output "---"
    Write-Output "> mixed load, worker, sharded, cached, $i threads"
    erl +S $i -noshell -s benchmark test_worker_sharded_cached_mixedload -s init stop | Out-File -Encoding utf8 "$DirName/work-shard-cached-mix$i.txt"
}

