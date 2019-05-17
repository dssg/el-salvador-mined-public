#!/bin/bash
# Arguments:
#	$1 - full file path to xlsx file
#	$2 - full file path to csv folder
#	$3 - number of headers to skip

in2csv -u 1 -H -f xlsx -e latin_1 $1 | csvformat -u 1 -e latin_1 -D "^" | tail -n +$(($3 + 2)) | gawk -v RS='"' 'NR % 2 == 0 { gsub(/\n/, "") } { printf("%s%s", $0, RT) }' > $2