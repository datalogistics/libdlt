#!/bin/bash


COUNTER = 0

while true
do
    for i in `seq 1 5`;
    do
	SC16 -I /opt/.osiris -O /opt/.osiris_cl_iu -V http://dev.crest.iu.edu -m SPREAD -o /dev/null -t 15 http://dev.crest.iu.edu:8888/exnodes/69c15a97-643c-4e03-a766-3ac2178a1716 &
    done
    wait
done
