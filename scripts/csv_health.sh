#!/bin/bash

# Arguments:
#	$1 file path

# Goes over a file and creates an array (seen) with key being the number of fields, and value the frequency of that key
a=($(awk -F'^' '{seen[NF]++}END{for(i in seen) print i}' $1))

# If the array has more than one value, return 0 (the file is unhealthy) else returns 1
if((${#a[@]}>1))
then
 echo 0
else
 echo 1
fi
