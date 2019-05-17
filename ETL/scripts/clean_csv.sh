#!/bin/bash

# Given an input file it removes \n inside "", as well as leading and trailing spaces inside ""
# Arguments
#     $1 Input file

cat $1 | sed 's/\" \+/\"/g' | sed 's/ \+\"/\"/g' | sed -e '1h;2,$H;$!d;g' -e ':loop' -e 's/\(\^\"[^\"]\+\)\(\n\+\)\([^\"]\+\"\^\)/\1 \3/' -e 't loop' > $2

/bin/bash csv_health.sh $2

# gawk -v RS='"' 'NR % 2 == 0 { gsub(/\n/, " ") } { printf("%s%s", $0, RT) }' 