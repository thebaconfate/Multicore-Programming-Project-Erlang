#!/usr/bin/env pwsh
erl -noshell -s $args[0] $args[1] -s init stop

