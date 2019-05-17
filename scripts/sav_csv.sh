#!/bin/bash

# Transforms an SAV file to csv, with ^ delimiters, and cell quotes (-u 1). \n within quotes, leading and trailing spaces are removed using clean_csv.sh
# The first two regex remove leading and trailing spaces inside fields. The third regex removes contiguous whitespaces. The final  regex removes empty strings
# Arguments:
#	$1 - input file path
#	$2 - output file path


if [ $# -eq 2 ]; then
    Rscript sav_csv.R $1 ./dum_file.csv
#     cat ./dum_file.csv | /bin/bash ./clean_csv.sh > $2
    cat ./dum_file.csv | sed 's/\([\"\^]\) \+/\1/g' | sed 's/ \+\([\"\^]\)/\1/g'| sed 's/  \+/ /g' | sed 's/\^\"\"\^/\^\^/g' > $2
    rm ./dum_file.csv
else
    echo "Two arguments are needed"
    echo "Run: sav_csv.sh <input_file> <output_file>"
fi
