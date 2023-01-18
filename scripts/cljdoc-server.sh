#! /usr/bin/env bash

# Utility script for starting the cljdoc web server.

set -euxo pipefail

# switch to project root directory if we're not already there
script_directory=`dirname "${BASH_SOURCE[0]}"`
cd "$script_directory/.."
project_root=$(pwd)

# Create dirs for the cljdoc local SQLite DB to live in

mkdir -p /tmp/cljdoc

# Make sure we have the latest version of the Docker image.

docker pull cljdoc/cljdoc

# Start the server.

cat <<EOF

Starting web server on port 8000. Leave this window open.

EOF

docker run --rm \
       -p 8000:8000 \
       --volume "$project_root:/toucan2" \
       --volume "$HOME/.m2:/root/.m2" \
       --volume /tmp/cljdoc:/app/data \
       cljdoc/cljdoc
