#! /bin/bash

RACE=-race

rm mr-*
rm temp*

go build $RACE -buildmode=plugin ../mrapps/wc.go
go build $RACE mrcoordinator.go
go build $RACE mrworker.go
