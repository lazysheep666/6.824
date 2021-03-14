#!/bin/bash

for ((i=0; i<=100; i=i+1)); do
  go test -run $1 -race
  if [ $? -eq 0 ]; then
      echo "ok $i"
    else
      exit 1
  fi
done
