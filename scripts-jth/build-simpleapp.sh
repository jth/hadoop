#!/usr/bin/env bash

PWD=`pwd`
HADOOP_SRC=$HOME/work/MA/repos/hadoop/
SIMPLE_SRC=$HADOOP_SRC/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/simple-yarn-app
SIMPLE_JAR=$SIMPLE_SRC/target/simple-yarn-app-1.1.0.jar
TARGET=/home/jth/work/MA/build/hadoop-3.0.0-SNAPSHOT/share/hadoop/yarn
# Command line arguments

cyan="\033[01;36m"
red="\033[01;31m"
bold="\033[1m"
restore="\033[0m"

log() {
    printf "$*\n"
    return $?
}

warn() {
   log "${bold}${red}WARNING: $*${restore}"
}

info() {
   log "${bold}${cyan}INFO: $*${restore}"
}

fail() {
   log "${bold}${red}ERROR: $*${restore}"
   exit 1
}

print_ok() {
    echo -e "\033[32mOK\033[0m"
}

change_dir() {
    if [ -z "$1" ]
    then
        fail "Invalid use of change_dir(). Exiting."
    else
        echo "Entering $1"
        cd $1
    fi
}

return_to_pwd() {
    echo "Leaving `pwd`"
    cd $PWD
}

build_simple_app() {
    change_dir $SIMPLE_SRC

    build_cmd="mvn clean package"

    info "Building simple-app (\033[01;36m$build_cmd\033[0m)..."

    if eval "$build_cmd" &> /tmp/build-log-simple; then
        print_ok
    else
        fail "Building simple-app failed. Look at /tmp/build-log-simple for details."
    fi

    info "Installing into maven cache..."
    if eval "mvn install &>> /tmp/build-log-simple"; then
        print_ok
    else
        fail "Installing simple app jar into maven cache failed. Look at /tmp/build-log-simple for details."
    fi

    return_to_pwd
}

deploy_simple_app() {
    info "Copying simple-app into $TARGET"
    if eval "cp $SIMPLE_JAR $TARGET"; then
        print_ok
    else
        fail "Copying $SIMPLE_JAR into $TARGET failed"
    fi
}

## main ##

build_simple_app
deploy_simple_app
