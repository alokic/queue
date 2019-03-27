#!/bin/bash

folder=$1
proto_folder="$PWD/$folder"
gen_folder=$proto_folder

cd $proto_folder
echo $GOPATH
echo "$PWD" && ls
protoc --proto_path=. --proto_path=/usr/local/include --proto_path=$GOPATH/src --proto_path=$GOPATH/src/github.com/gogo/protobuf/gogoproto --proto_path=$proto_folder --gogo_out=plugins=grpc:$gen_folder *.proto
