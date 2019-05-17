for f in *.csv
do
    fn="${f%.*}"
    mv $f "$fn""-B.csv"
done