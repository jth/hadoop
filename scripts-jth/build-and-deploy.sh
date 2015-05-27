#!/usr/bin/env bash

PWD=`pwd`
HADOOP_SRC=$HOME/work/MA/repos/hadoop/
YARN_SRC=$HADOOP_SRC/hadoop-yarn-project/
TARGET=$HOME/work/MA/build/
HADOOP_TAR=$HADOOP_SRC/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT.tar.gz
YARN_TAR=$HADOOP_SRC/hadoop-yarn-project/target/hadoop-yarn-project-3.0.0-SNAPSHOT.tar.gz

# Command line arguments
REBUILD=false
VERBOSE=false

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

build_hadoop() {
    change_dir $HADOOP_SRC
    tmp=${HADOOP_TAR#*/}

    if [[ -e $HADOOP_TAR ]]; then
        info "Deleting ${tmp:1}"
        rm -f $HADOOP_TAR
    fi
    
    build_cmd="mvn clean package -Pdist -Psrc -DskipTests -Dtar -Dmaven.javadoc.skip=true"

    info "Building hadoop (\033[01;36m$build_cmd\033[0m)..."

    if eval "$build_cmd" &> /tmp/build-log-hadoop; then
        print_ok
    else
        fail "Building hadoop failed. Look at /tmp/build-log-hadoop for details."
    fi

    info "Installing into maven cache..."
    if eval "mvn install -DskipTests -Dmaven.javadoc.skip=true &>> /tmp/build-log-hadoop"; then
        print_ok
    else
        fail "Installing hadoop jars into maven cache failed. Look at /tmp/build-log/hadoop for details."
    fi

    return_to_pwd
}

deploy_hadoop() {
    info "Extracting `basename $HADOOP_TAR` to $TARGET... "
    if [[ ! -d $TARGET ]]; then
	warn "$TARGET doesn't existing. Creating it."
	mkdir -p $TARGET
    fi

    if tar -xzf $HADOOP_TAR -C $TARGET; then
        print_ok
    else
        fail "Extraction failed. Exiting."
    fi
}

build_yarn() {
    change_dir $YARN_SRC

    build_cmd="mvn -T 8 package -DskipTests -Dmaven.javadoc.skip=true"
    echo -e "Building YARN (\033[01;36m$build_cmd\033[0m)..."

    if eval "$build_cmd" &> /tmp/build-log-yarn; then
        print_ok
    else
        fail "Building yarn failed. Look at /tmp/build-log-yarn for details."
    fi

    return_to_pwd
}

deploy_yarn() {
    info "Extracting `basename $YARN_TAR` to /tmp"
    if tar -xzf $YARN_TAR -C /tmp/
    then
        print_ok
    else
        fail "Extraction of YARN failed."
    fi
    echo -e -n "Copying YARN share-folder into $TARGET/hadoop-3.0.0-SNAPSHOT/"

    if eval "cp -r /tmp/hadoop-yarn-project-3.0.0-SNAPSHOT/share/ $TARGET/hadoop-3.0.0-SNAPSHOT/"; then
        print_ok
    else
        fail "Copying of YARN failed"
    fi

    return_to_pwd
}

usage() {
    echo "$0 [-r] [-v]"
    echo "  No arguments: Just build and deploy YARN into existing HADOOP installation"
    echo "  -r: Complete hadoop rebuild rebuild (mvn clean package -Pdist,native,docs,src -DskipTests -Dtar)"
    echo "  -v: Verbose output"
}

## main ##

main() {
# Default is just building yarn.
    if [[ $REBUILD == true ]]; then
        warn "Doing complete rebuild of hadoop"

        if [[ -e $TARGET/hadoop-3.0.0-SNAPSHOT ]]; then
            warn "Deleting $TARGET/hadoop-3.0.0-SNAPSHOT"
            rm -rf $TARGET/hadoop-3.0.0-SNAPSHOT
        fi

        build_hadoop
        deploy_hadoop
    else
        info "Just rebuilding YARN"

        build_yarn
        deploy_yarn
    fi
    exit
}

while [[ $# -ge 1 ]]
do
key="$1"
case $key in 
    -r)
    REBUILD=true
    ;;
    -v)
    VERBOSE=true
    ;;
    *)
    usage
    exit
    ;;
esac
shift
done

echo "Are you sure you want to rebuild YARN?"
select yn in "Yes" "No"; do
    case $yn in
        Yes)
        main
        ;;
        No)
        exit
        ;;
    esac
done
