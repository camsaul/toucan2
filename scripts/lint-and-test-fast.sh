#! /usr/bin/env bash

# This script is a convenience for running linters and tests without having to type in a bunch of nonsense.

set -euxo pipefail

codespell

clj-kondo --parallel --lint src test toucan1/src/ toucan1/test/

clojure -X:dev:test:test-h2 $@
