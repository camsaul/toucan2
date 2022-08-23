#! /usr/bin/env bash

# Utility script for building + ingesting dox with cljdoc locally.

set -euxo pipefail

# switch to project root directory if we're not already there
script_directory=`dirname "${BASH_SOURCE[0]}"`
cd "$script_directory/.."
project_root=$(pwd)

version=$(cat VERSION.txt)

# Build and install locally.

clj -T:build build-and-install

# Create dirs for the cljdoc local SQLite DB to live in

mkdir -p /tmp/cljdoc

# ingest the dox

docker run --rm \
       --volume "$project_root:/toucan2" \
       --volume "$HOME/.m2:/root/.m2" \
       --volume /tmp/cljdoc:/app/data \
       --entrypoint clojure \
       cljdoc/cljdoc \
       -M:cli ingest \
         --project io.github.camsaul/toucan2 \
         --version "$version" \
         --git /toucan2

cat <<EOF

Docs ingested.

If you haven't already, start the web server with ./scripts/cljdoc-server.sh and leave the window open.

You can view the docs locally by going to

http://localhost:8000/d/io.github.camsaul/toucan2/$version

EOF
