#! /bin/bash

set -exo pipefail

script_directory=`dirname "${BASH_SOURCE[0]}"`
cd "$script_directory"
script_directory=`pwd`

version="$1"

if [ ! "$version" ]; then
    echo "Usage: ./install-all.sh [version]"
    exit -1
fi

# ./build-all.sh "$version"

install() {
    what="$1"
    echo "Install $what"
    cd "$script_directory"
    rm -f pom.xml
    cp "$script_directory/$what/pom.xml" .
    clojure -X:install :artifact "\"target/$what.jar\""
}

install_all() {
    install toucan2-core
    install toucan2-honeysql
    install toucan2-jdbc
    install toucan2
}

install_all
