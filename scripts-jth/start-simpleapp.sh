#!/usr/bin/env bash

PWD=`pwd`
HADOOP_HOME=$HOME/work/MA/build/hadoop-3.0.0-SNAPSHOT
UM_LAUNCHER=share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-3.0.0-SNAPSHOT.jar
SIMPLE_JAR=share/hadoop/yarn/simple-yarn-app-1.1.0.jar
HADOOP_BIN=./bin/hadoop

# Command line arguments
cyan="\033[01;36m"
red="\033[01;31m"
bold="\033[1m"
restore="\033[0m"

return_to_pwd() {
    echo "Leaving `pwd`"
    cd $PWD
}

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
   return_to_pwd
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

start_yarn() {
    if [ -e "/tmp/yarn-running" ]; then
        log "Yarn already running"
        return
    fi
    
    if eval "$HADOOP_HOME/sbin/start-yarn.sh"; then
        print_ok
        touch /tmp/yarn-running
    else
        fail "Could not execute start-yarn.sh"
    fi
}

build_app() {
    log "Building C-part of yarn application"
    cd $HOME/work/MA/repos/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/simple-yarn-app/src/main/c/
    mkdir build
    if eval "cmake .."; then
        print_ok
    else
        fail "cmake in `pwd` failed"
    fi

    if eval "make"; then
       print_ok
    else
       fail "make in `pwd` failed"
    fi
}

# $1 = number of containers
start_simple_app() {
    change_dir $HADOOP_HOME    

    if [[ ! -x "$HOME/work/MA/repos/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/simple-yarn-app/src/main/c/build/pi" ]]; then 
	build_app
    fi

    cmd="$HADOOP_BIN jar $UM_LAUNCHER Client -classpath $SIMPLE_JAR \
    -cmd \"java de.jth.simpleyarnapp.ApplicationMaster $1\""

    info "Executing $cmd"

    if eval "$cmd"; then
        print_ok
    else
        fail "Executing simple app failed"
    fi

    return_to_pwd
}

## main ##

if [ $# -ne 1 ]; then
    fail "Provide the number of containers as argument"
else
    start_yarn
    start_simple_app $1
fi
