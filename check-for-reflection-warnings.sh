#! /usr/bin/env bash

printf "\e[1;34mChecking for reflection warnings. This may take a few minutes, so sit tight...\e[0m\n"

warnings=`lein check-reflection-warnings 2>&1 | grep Reflection | grep toucan2 | sort | uniq`

if [ ! -z "$warnings" ]; then
    printf "\e[1;31mYour code has introduced some reflection warnings.\e[0m 😞\n"
    echo "$warnings";
    exit -1;
fi

printf "\e[1;32mNo reflection warnings! Success.\e[0m\n"
