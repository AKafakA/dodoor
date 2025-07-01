#!/bin/bash
rm -rf src/main/gen-java
thrift -o src/main/ --gen java Thrift/src/types.thrift
thrift -o src/main/ --gen java Thrift/src/service.thrift
#mvn clean package
#mvn compile
#mvn package -Dmaven.test.skip=true
