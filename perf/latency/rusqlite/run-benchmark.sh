#!/bin/bash

for i in $(seq 10 100 10)
do
  ./target/release/rusqlite-multitenancy $i >> results.csv
done
