#!/usr/bin/env pwsh

$ErrorActionPreference = "Stop"
$DirName = "benchmarks"

if (-Not (Test-Path $DirName)) {
    mkdir $DirName | Out-Null
}


for ($i = 1; $i -le 64; $i++) {
    Write-Output "---"
    Write-Output "> fib, $i threads"
    erl +S $i -noshell -s benchmark test_fib -s init stop | Out-File -Encoding utf8 "benchmarks/result-fib-$i.txt"

    Write-Output "---"
    Write-Output "> readwrite, $i threads"
    erl +S $i -noshell -s benchmark test_readwrite -s init stop | Out-File -Encoding utf8 "benchmarks/result-readwrite-$i.txt"

    Write-Output "---"
    Write-Output "> readonly, $i threads"
    erl +S $i -noshell -s benchmark test_readonly -s init stop | Out-File -Encoding utf8 "benchmarks/result-readonly-$i.txt"

    Write-Output "---"
    Write-Output "> mixed load, $i threads"
    erl +S $i -noshell -s benchmark test_mixedload -s init stop | Out-File -Encoding utf8 "benchmarks/result-mixedload-$i.txt"
}

