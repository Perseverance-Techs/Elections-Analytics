#!/bin/sh
# This is a comment!
while true
do
 python3  loadVoterstodatalake.py 1 &
 python3  loadVoterstodatalake.py 2 &
 python3  loadVoterstodatalake.py 3 &
 python3  loadVoterstodatalake.py 4 &

 sleep 20
 pkill -f loadVoterstodatalake.py
 sleep 5
done
# This is a comment, too!
