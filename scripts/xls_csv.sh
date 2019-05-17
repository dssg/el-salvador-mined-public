#!/bin/bash

# Transforms an Excel file to csv, with latin_1 encoding, ^ delimiters, and cell quotes (-u 1). A row of alphabetic headers (-H) is added and then removed (tail -n +2). \n within quotes, leading and trailing spaces are removed using clean_csv.sh
# After the file is created, it's "health" is checked by checking whether all rows have the same number of fields
# Arguments:
#	$1 - input file path
#	$2 - output file path

if [ $# -eq 2 ]; then
#     in2csv -u 1 -H -e latin_1 $1 | csvformat -u 1 -e latin_1 -D "^" | tail -n +2 | /bin/bash ./clean_csv.sh > $2
    in2csv -u 1 -H -e UTF-8 $1 | csvformat -u 1 -e UTF-8 -D "^" | tail -n +2 | /bin/bash ./clean_csv.sh > $2
else
    echo "Two arguments are needed"
    echo "Run: xls_csv.sh <input_file> <output_file>"
fi
