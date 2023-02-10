#! /usr/bin/env bash

# This script is a convenience for running linters and tests without having to type in a bunch of nonsense.

# switch to project root directory if we're not already there
script_directory=`dirname "${BASH_SOURCE[0]}"`
cd "$script_directory/.."

set -euxo pipefail

codespell

clojure -X:dev:test $@
