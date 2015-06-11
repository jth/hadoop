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
