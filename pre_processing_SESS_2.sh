for f in *EXP_SESS_LOG_NODEP*;
do
  echo "Processing $f file..";
  gunzip $f
done
for f in *EXP_SESS_LOG_NODEP*;
do
  echo "Processing $f file..";
  tr  -d '\000' < $f > $f.csv
  rm -f $f
done

