#! /bin/bash

set -exo pipefail

script_directory=`dirname "${BASH_SOURCE[0]}"`
cd "$script_directory"
script_directory=`pwd`

version="$1"

if [ ! "$version" ]; then
    echo "Usage: ./deploy-all.sh [version]"
    exit -1
fi

# ./build-all.sh "$version"

deploy() {
    what="$1"
    echo "Deploy $what"
    cd "$script_directory"
    rm -f pom.xml
    cp "$script_directory/$what/pom.xml" .
    clojure -X:deploy :artifact "\"target/$what.jar\""
}

deploy_all() {
    deploy toucan2-core
    deploy toucan2-honeysql
    deploy toucan2-jdbc
    deploy toucan2
}

deploy_all
