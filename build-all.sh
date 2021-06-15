#! /bin/bash

set -exo pipefail

rm -rf target
rm -f pom.xml

for file in `find . -name '.cpcache'`; do
    rm -rf "$file"
done

script_directory=`dirname "${BASH_SOURCE[0]}"`
cd "$script_directory"
script_directory=`pwd`

version="$1"

if [ ! "$version" ]; then
    echo "Usage: ./build-all.sh [version]"
    exit -1
fi

build_jar () {
    clojure -X:jar :version "\"$version\""
}

install() {
    what="$1"
    echo "Install $what"
    cd "$script_directory"
    rm -f pom.xml
    cp "$script_directory/$what/pom.xml" .
    clojure -X:install :artifact "\"target/$what.jar\""
}

build_all () {
    echo "Build toucan2-core"
    cd "$script_directory"/toucan2-core
    rm -f pom.xml
    clojure -Spom
    build_jar
    install toucan2-core

    echo "Build toucan2-honeysql"
    cd "$script_directory"/toucan2-honeysql
    rm -f pom.xml
    clojure -Sdeps "{:deps {com.camsaul/toucan2-core {:mvn/version \"$version\"}}}" -Spom
    build_jar
    install toucan2-honeysql

    echo "Build toucan2-jdbc"
    cd "$script_directory"/toucan2-jdbc
    rm -f pom.xml
    clojure -Sdeps "{:deps {com.camsaul/toucan2-core {:mvn/version \"$version\"}}}" -Spom
    build_jar
    install toucan2-jdbc

    echo "Build toucan2"
    cd "$script_directory"/toucan2
    rm -f pom.xml
    clojure -Sdeps "{:deps {com.camsaul/toucan2-core {:mvn/version \"$version\"}
                            com.camsaul/toucan2-honeysql {:mvn/version \"$version\"}
                            com.camsaul/toucan2-jdbc {:mvn/version \"$version\"}}}" \
            -Spom
    build_jar
    install toucan2

    echo "Build toucan2-toucan1"
    cd "$script_directory"/toucan2-toucan-1
    rm -f pom.xml
    clojure -Sdeps "{:deps {com.camsaul/toucan2 {:mvn/version \"$version\"}}}" -Spom
    build_jar
    install toucan2-toucan1
}

build_all
