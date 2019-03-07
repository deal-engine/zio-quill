#!/usr/bin/env bash

set -e

export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5432

export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306

export SQL_SERVER_HOST=127.0.0.1
export SQL_SERVER_PORT=11433

export ORACLE_HOST=127.0.0.1
export ORACLE_PORT=11521

export CASSANDRA_HOST=127.0.0.1
export CASSANDRA_PORT=19042

export ORIENTDB_HOST=127.0.0.1
export ORIENTDB_PORT=12424

export SBT_ARGS="-Dquill.macro.log=false -Doracle=true -Xms1024m -Xmx3g -Xss5m -XX:ReservedCodeCacheSize=256m -XX:+TieredCompilation -XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC ++$TRAVIS_SCALA_VERSION"

modules=$1

echo "Modules: $modules"

function show_mem() {
    free -m | awk 'NR==2{printf "Memory Usage: %s/%sMB (%.2f%%)\n", $3,$2,$3*100/$2 }'
    # mem_details
    # free_details
    # docker_stats
}
export -f show_mem

function mem_details() {
    echo "===== System Memory Stats Start ====="
    ps -eo size,rss,%mem,cmd --sort=-size | head -20 | awk '{ hr=$1/1024; rr=$2/1024; printf("%13.2fMb ",hr); printf("%13.2fMb ",rr); print(" " $0) }'
    sleep 2
    echo "===== System Memory Stats End ====="
}
export -f mem_details

function free_details() {
    echo "===== Free Memory Stats Start ====="
    free
    sleep 2
    echo "===== Free Memory Stats End ====="
}
export -f free_details

function docker_stats() {
    echo "===== Docker Stats Start ====="
    docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" --no-stream
    sleep 4
    echo "===== Docker Stats End ====="
}
export -f docker_stats

if [[ $TRAVIS_SCALA_VERSION == 2.11* ]]; then
    export SBT_ARGS="$SBT_ARGS coverage"
fi

function wait_for_databases() {
    show_mem

    # Start sbt compilation and database setup in parallel
    sbt clean scalariformFormat test:scalariformFormat -Dmodules=base $SBT_ARGS test & COMPILE=$!
    ./build/setup_databases.sh & SETUP=$!

    # Wait on database setup. If it has failed then kill compilation process and exit with error
    wait $SETUP

    if [[ "$?" != "0" ]]; then
       echo "Database setup failed"
       sleep 10
       kill -9 $COMPILE
       exit 1
    fi
    echo "Database Setup is finished, waiting for the compilation of core module"

    wait $COMPILE
    if [[ "$?" != "0" ]]; then
       echo "Compilation of core module has failed"
       sleep 10
       exit 1
    fi

    echo "Database Setup is finished"
    show_mem
}

function wait_for_bigdata() {
    show_mem

    sbt clean scalariformFormat test:scalariformFormat $SBT_ARGS quill-coreJVM/test:compile & COMPILE=$!
    ./build/setup_bigdata.sh & SETUP=$!

    wait $SETUP
    if [[ "$?" != "0" ]]; then
       echo "BigData Database setup failed"
       sleep 10
       kill -9 $COMPILE
       exit 1
    fi

    wait $COMPILE
    if [[ "$?" != "0" ]]; then
       echo "Compilation of core module has failed"
       sleep 10
       exit 1
    fi

    echo "BigData Database Setup is finished"
    show_mem
}

function db_build() {
    wait_for_databases
    sbt -Dmodules=db $SBT_ARGS test doc
}

function bigdata_build() {
    wait_for_bigdata
    sbt -Dmodules=bigdata $SBT_ARGS test doc
}

function full_build() {
    wait_for_databases
    wait_for_bigdata
    sbt $SBT_ARGS test tut doc
}

if [[ $modules == "db" ]]; then
    echo "Build Script: Doing Database Build"
    db_build
elif [[ $modules == "bigdata" ]]; then
    echo "Build Script: Doing BigData Build"
    bigdata_build
else
    echo "Build Script: Doing Full Build"
    full_build
fi

show_mem
echo "Tests completed. Shutting down"
time docker-compose down
# for 2.11 publish coverage
if [[ $TRAVIS_SCALA_VERSION == 2.11* ]]; then
    echo "Coverage"
    time sbt $SBT_ARGS coverageReport coverageAggregate
    pip install --user codecov && codecov
fi
show_mem

sleep 10