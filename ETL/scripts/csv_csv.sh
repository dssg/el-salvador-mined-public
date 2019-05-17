#!/bin/bash

# Takes a .csv file in latin-1 and converts it to UTF-8 (possible downfall is that csvkit might not catch all non-UTF8 characters)
# Once in UTF-8, the first two regex remove leading and trailing spaces inside fields. The third regex removes contiguous whitespaces. The final two regex remove empty strings, and it's run twice because for some reason it's not catching all of them the first time

# Arguments:
#	$1 - input .csv file path
#	$2 - output .csv file path


if [ $# -eq 2 ]; then
        iconv --from-code L1 -t UTF-8 $1 | sed 's/\([\",]\) \+/\1/g' | sed 's/ \+\([\",]\)/\1/g'| sed 's/  \+/ /g' | sed 's/,\"\",/,,/g' | sed 's/,\"\",/,,/g' > $2

else
    echo "Two arguments are needed"
    echo "Run: csv_csv.sh <input_file> <output_file>"
fi