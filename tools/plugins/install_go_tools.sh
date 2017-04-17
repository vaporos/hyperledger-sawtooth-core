#!/bin/bash -x

. /vagrant/conf.sh

set -e

GOPATH=/home/$VAGRANT_USER/go
GOBIN=$GOPATH/bin

export GOPATH
echo "export GOPATH=$GOPATH:/project/sawtooth-core/sdk/go" >> /home/$VAGRANT_USER/.bashrc
echo "export PATH=$PATH:$GOBIN" >> /home/$VAGRANT_USER/.bashrc

apt-get install -y -q \
    golang

go get -u \
    github.com/golang/protobuf/proto \
    github.com/golang/protobuf/protoc-gen-go \
    github.com/pebbe/zmq4

chown -R $VAGRANT_USER:$VAGRANT_USER $GOPATH
chown -R $VAGRANT_USER:$VAGRANT_USER $GOBIN
