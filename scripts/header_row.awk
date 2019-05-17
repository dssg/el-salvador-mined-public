# Finds the first row where the number of not empty fields is (NF-ARGV[2]), which we assume is the row containing the headers

BEGIN{FS="^"}  # Change ^ to whatever value separator you used in your file
{
 if(NR<40){  # Go over only the first 40 rows
  n=0;
  for(i=1; i<=NF; i++){  # Iterate inside each row and increase n by one if the field is not empty
   if($i!=""){n++}
  }
  if(n>=NF-ARGV[2]){
   print NR; exit # As soon as n is equal to the total number of fields, return the row number and exit
  }
 }
 else{print "WARNING: could not find headers in the first 40 rows"; exit}
}
END{}