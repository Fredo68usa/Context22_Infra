cd /opt/AIM_Dam/AIM_Python/Routines
source env/bin/activate
cd BUFF__Enrichment
python3 BUFF_Enrich_4.py
cd /opt/sonarg/AIM/ToBeProcessed/
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
cd /opt/AIM_Dam/AIM_Python/Routines
source env/bin/activate
cd BUFF__Enrichment
python3 BUFF_Enrich_4.py

