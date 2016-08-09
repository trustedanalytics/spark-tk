#!/usr/bin/env bash


# cleans the stuff from integration tests

NAME="[`basename $0`]"
DIR="$( cd "$( dirname "$0" )" && pwd )"
echo "$NAME DIR=$DIR"

echo "$NAME rm tests/test_docs_generated.py"
rm -f tests/test_docs_generated.py

echo "$NAME rm -r tests/sandbox/*"
rm -rf tests/sandbox/*
