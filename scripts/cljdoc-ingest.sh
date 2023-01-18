#! /usr/bin/env bash

# Utility script for building + ingesting dox with cljdoc locally.

set -euxo pipefail

# switch to project root directory if we're not already there
script_directory=`dirname "${BASH_SOURCE[0]}"`
cd "$script_directory/.."
project_root=$(pwd)

major_minor_version=$(cat VERSION.txt)
patch_version=$(git rev-list HEAD --count)
version="$major_minor_version.$patch_version"

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
       -Sforce \
       -M:cli ingest \
         --project io.github.camsaul/toucan2 \
         --version "$version" \
         --git /toucan2 \
         --rev $(git rev-parse HEAD)

cat <<EOF

Docs ingested.

If you haven't already, start the web server with ./scripts/cljdoc-server.sh and leave the window open.

You can view the docs locally by going to

http://localhost:8000/d/io.github.camsaul/toucan2/$version

If something didn't work, try going to

http://localhost:8000/builds

EOF
