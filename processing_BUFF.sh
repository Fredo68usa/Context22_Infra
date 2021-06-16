cd /xxxx
source env/bin/activate
cd BUFF__Enrichment
python3 BUFF_Enrich_4.py
cd /ToBeProcessed/
for f in *EXP_BUFF_USAGE*;
do
  echo "Processing $f file..";
    gunzip $f
done
for f in *EXP_BUFF_USAGE*;
do
  echo "Processing $f file..";
  tr  -d '\000' < $f > $f.csv
  rm -f $f
done

source env/bin/activate
cd BUFF__Enrichment
python3 BUFF_Enrich_4.py
