#!/bin/bash

goapp_folder=$GOPATH/src/github.com/honestbee/knowledge-graph
cmd_folder=$goapp_folder/cmd
scripts_folder=$goapp_folder/scripts
tmp_folder=/tmp
gobin=$GOPATH/bin


usage()
{
    print_info "ENV environment variable is not set"
}


check_var()
{
    if [ -z "$1" ];
    then
        print_error "$2 not given"
        usage
        exit 1
    fi
}

error()
{
    echo "error $@"
    exit "${3:-1}"
}
trap error ERR

print_info()
{
    echo ""
    echo "${0##*/}: $1"
    echo "================================="
}

print_error()
{
    echo ""
    echo "${0##*/}: ************ $1 *****************"
}