#!/usr/bin/env bash

NAME="${1}"

grep -v -E '^[[:space:]]*$' $NAME.csv > $NAME.cleaned.csv