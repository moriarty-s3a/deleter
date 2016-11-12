#!/usr/bin/env bash

BASEDIR=/tmp/foo

for company in 0 1 2 3 4 5 6; do
  for device in dev0 dev1 dev2 dev3; do
    for year in 2000 2014 2015 2016; do
      for month in 2 6 8 10 11 12; do
        for day in 2 10 11 12; do
          for hour in 3 9 13 20 22; do
            for min in 2 16 32; do
              mkdir -p $BASEDIR/$company/$device/$year/$month/$day/$hour/$min
              touch $BASEDIR/$company/$device/$year/$month/$day/$hour/$min/Alice
              touch $BASEDIR/$company/$device/$year/$month/$day/$hour/$min/Bob
              touch $BASEDIR/$company/$device/$year/$month/$day/$hour/$min/Charlie
            done
          done
        done
      done
    done
  done
done
