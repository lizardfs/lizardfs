#!/bin/bash

set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <max number of parameters to be supported>"
    exit 1
else
    MAX=$1
fi


cat << END
#pragma once

#include "config.h"

// DO NOT MODIFY THIS FILE BY HAND!
//
// This file was automatically generated with $(basename $0). If you want macros below to
// support more then $MAX parameters, generate this file once again.

END

for i in $(seq 1 $MAX); do
    line="#define APPLY1_$i(Macro, Sep"
    for j in $(seq $i); do
        line+=", t$j"
    done
    line+=") Macro(t1)"
    if [ $i -gt 1 ]; then
        line+=" Sep() APPLY1_$((i-1))(Macro, Sep"
        for j in $(seq 2 $i); do
            line+=", t$j"
        done
        line+=")"
    fi
    echo $line
done
echo
for i in $(seq 1 $MAX); do
    line="#define APPLY2_$((2*i))(Macro, Sep"
    for j in $(seq $i); do
        line+=", T$j, t$j"
    done
    line+=") Macro(T1, t1)"
    if [ $i -gt 1 ]; then
        line+=" Sep() APPLY2_$((2*i-2))(Macro, Sep"
        for j in $(seq 2 $i); do
            line+=", T$j, t$j"
        done
        line+=")"
    fi
    echo $line
done
echo
for i in $(seq 1 $MAX); do
    line="#define VARS_COMMAS_$((2*i))(T1, t1";
    for j in $(seq 2 $i); do
        line+=", T$j, t$j";
    done;
    line+=") t1"
    for j in $(seq 2 $i); do
        line+=", t$j";
    done;
    echo $line;
done
echo
line="#define COUNT_ARGS(...) COUNT_ARGS_(, ##__VA_ARGS__"
for i in $(seq $((MAX * 2)) -1 0); do
    line="${line}, $i"
done
line="${line})"
echo $line
echo
line="#define COUNT_ARGS_("
for i in $(seq 0 $((MAX * 2 ))); do
    line="${line}a$i, "
done
line="${line}count, ...) count"
echo $line
echo
echo "// File generated correctly."
