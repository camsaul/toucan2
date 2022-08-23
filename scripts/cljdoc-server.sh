#! /usr/bin/env bash

# Utility script for starting the cljdoc web server.

set -euxo pipefail

# Create dirs for the cljdoc local SQLite DB to live in

mkdir -p /tmp/cljdoc

# Start the server.

docker run --rm \
       -p 8000:8000 \
       --volume /tmp/cljdoc:/app/data \
       cljdoc/cljdoc

cat <<EOF

Web server started on port 8000. Leave this window open.

EOF
