#!/bin/bash

# Splits a delimiter-separated value file into headers and body. 
# Two options are provided:
#    1. Automatically detect the position of the headers using header_row.awk, which requires caret-separated value files. It finds the first row that is almost completely populated and considers it to be the headers. 
#    2. Split the headers at a predetermined position given by the user.

# Arguments:
#	$1 - file path with no extension
#	$2 - Whether to split automatically or not
#	$3 - If automatic: threshold cut for the header_row AWK file. Determines how many empty cells we can tolerate in the header detection
#        If not automatic: row at which to separate header and body


if [ $# -ne 3 ]; then
    echo "Three arguments are needed"
    echo "Run: header_split.sh <input_file_no_extension> <auto_split> <header_threshold>"
else
    if [ "$2" == 1 ]; then
        echo Splitting automatically
        headcut=$(awk -f header_row.awk $1".csv" $3)
        echo "Split at: ", $headcut
        
        tailcut=$(($headcut + 1))
        head -"$headcut" $1".csv" > $1"_head.csv"
        tail -n +"$tailcut" $1".csv" > $1"_body.csv"
    else
        echo Splitting at $3
        headcut=$3
        tailcut=$(($headcut + 1))
        head -"$headcut" $1".csv" | tail -1 > $1"_head.csv"
        tail -n +"$tailcut" $1".csv" > $1"_body.csv"
    fi
fi