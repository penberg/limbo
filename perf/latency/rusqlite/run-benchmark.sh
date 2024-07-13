#!/bin/bash

for i in {10..100..10}
do
  ./target/release/rusqlite-multitenancy $i >> results.csv
done
