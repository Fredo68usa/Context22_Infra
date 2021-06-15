import csv
import pymongo
import urllib.parse
import json
import getpass
import datetime as dt
import time
import os
import hashlib
import calendar
import socket
import glob
import sys

csvFiles=[]
myListCollectors = []
DataFiles=[]
DataFile=[]
# path = '../DamData/ToBeProcessed'
path = '/opt/sonarg/AIM/ToBeProcessed/'
pathProcessed = '/opt/sonarg/AIM/ToBeProcessed/Processed/'
pathlength=len(path)
ticks_before = time.time()
fullSQL = []
DAMDataRec = []
BuffNum = []
lineDict={}
doc_count=0
doc_count_total=0
perfDict={}
perfList=[]
# ---- Declaration of hash
m = hashlib.md5()

# --- Opening Mongo
def Open_Mongo():

   # --- Password ----
   # try:
   #     p = getpass.getpass()
   # except Exception as error:
   #      print('ERROR', error)
   # else:
   #      print('Password entered')

   # ----- Connection
   # username = urllib.parse.quote_plus('fpetit')
   username = 'fpetit'
   password = 'RoKAmadur2025!'

   global ticks_before
   ticks_before = time.time()
   myclient = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1:27117/admin' % (username, password))
   return(myclient)

# --- Metadata  ----
def MetaData(myclient):

    mydb = myclient["sonargd"]
    mycol = mydb["A_COLLECTORS"]
    for row in mycol.find():
            myListCollectors.append(row)

def DataFile_List():
    # r=root, d=directories, f = files
    global DataFiles
    global DataFile
    csvFiles=glob.glob(path + "*BUFF_USAGE*.csv")
    if len(csvFiles) == 0:
       print ("No File to Process")
       sys.exit()
    for file in csvFiles:
       if "BUFF_USAGE" in file:
         COLL = file.split('_')[1]
         DataFile.append(COLL)
         DataFile.append(file)
         DataFiles.append(DataFile)
         DataFile=[]

# ---- Lookups into the Metadata  -----

    
def lookup_A_COLLECTOR(COLL):
    if COLL != None:
       # print ("Youpi ...",SRC_PRG)
       for i in range(0,len(myListCollectors)):

           if myListCollectors[i]['Collectors'] in COLL:
              return(myListCollectors[i])


# --- Enrich line with Metqadata
def enrich_by_metadata(line):

    # --- Collector
    if "SonarG Source" in line:
       COLL = line['SonarG Source']
    else:
       COLL = None

    sel_metadata = lookup_A_COLLECTOR(COLL)
    line["Collector Metadata"] = sel_metadata

    # --- Return ---
    return(line)


# --- Process line ----
def enrich_line(line):
      # --- Timestamp
      ts = line['Timestamp']
      # print ('Timestamp' , ts)
      utc_h = int(line["UTC Offset"])
      # new_ts=dt.datetime.strptime(ts[:19],'%Y-%m-%dT%H:%M:%S')
      new_ts=dt.datetime.strptime(ts,'%Y-%m-%d %H:%M:%S')
      line['Timestamp'] = new_ts + dt.timedelta(hours=abs(utc_h))
      line['Timestamp Local Time'] = new_ts

      # -----------
      line_meta = enrich_by_metadata(line)

      DayOfWeek=line_meta['Timestamp Local Time'].weekday()
      line_meta['DayOfWeek']=calendar.day_name[DayOfWeek]
      DayOfYear=line_meta['Timestamp Local Time'].timetuple().tm_yday
      line_meta['DayOfYear']=DayOfYear
      WeekOfYear=line_meta['Timestamp Local Time'].isocalendar()[1]
      line_meta['WeekOfYear']=WeekOfYear
      return(line_meta)

# --- num_line
def num_line(num_line):
    # num_line['% CPU Sniffer']=int( num_line['% CPU Sniffer'])
    num_line["% CPU Sniffer"]=int( num_line["% CPU Sniffer"])
    num_line["% Mem Sniffer"]=int( num_line["% Mem Sniffer"])
    num_line["% CPU Mysql"]=int( num_line["% CPU Mysql"])
    num_line["% Mem Mysql"]=int( num_line["% Mem Mysql"])
    num_line["Mem Sniffer"]=int( num_line["Mem Sniffer"])
    # num_line["Time Sniffer"]=int( num_line["Time Sniffer"])
    num_line["Free Buffer Space"]=int( num_line["Free Buffer Space"])
    num_line["Analyzer Rate"]=int( num_line["Analyzer Rate"])
    num_line["Logger Rate"]=int( num_line["Logger Rate"])
    num_line["Analyzer Queue Length"]=int( num_line["Analyzer Queue Length"])
    num_line["Analyzer Total"]=int( num_line["Analyzer Total"])
    num_line["Logger Queue Length"]=int( num_line["Logger Queue Length"])
    num_line["Logger Total"]=int( num_line["Logger Total"])
    num_line["Session Queue Length"]=int( num_line["Session Queue Length"])
    num_line["Session Total"]=int( num_line["Session Total"])
    num_line["ALP"]=int( num_line["ALP"])
    num_line["Eth0 Received"]=int( num_line["Eth0 Received"])
    num_line["Eth0 Sent"]=int( num_line["Eth0 Sent"])
    # num_line["Logger Dbs Monitored"]=int( num_line["Logger Dbs Monitored"])
    num_line["Logger Packets Ignored By Rule"]=int( num_line["Logger Packets Ignored By Rule"])
    num_line["Logger Session Count"]=int( num_line["Logger Session Count"])
    num_line["Mysql Disk Usage"]=int( num_line["Mysql Disk Usage"])
    num_line["Mysql Is Up"]=int( num_line["Mysql Is Up"])
    num_line["Promiscuous Received"]=int( num_line["Promiscuous Received"])
    num_line["Sniffer Connections Ended"]=int( num_line["Sniffer Connections Ended"])
    num_line["Sniffer Connections Used"]=int( num_line["Sniffer Connections Used"])
    num_line["SPD"]=int( num_line["SPD"])
    num_line["Sniffer Packets Ignored"]=int( num_line["Sniffer Packets Ignored"])
    num_line["Sniffer Packets Throttled"]=int( num_line["Sniffer Packets Throttled"])
    num_line["System Cpu Load"]=int( num_line["System Cpu Load"])
    num_line["System Memory Usage"]=int( num_line["System Memory Usage"])
    num_line["System Root Disk Usage"]=int( num_line["System Root Disk Usage"])
    num_line["System Uptime"]=int( num_line["System Uptime"])
    num_line["System Var Disk Usage"]=int( num_line["System Var Disk Usage"])
    num_line["Sessions Normal"]=int( num_line["Sessions Normal"])
    num_line["Session Timeout"]=int( num_line["Session Timeout"])
    num_line["Session Ignored"]=int( num_line["Session Ignored"])
    num_line["Session Direct Closed"]=int( num_line["Session Direct Closed"])
    num_line["Session Guessed"]=int( num_line["Session Guessed"])
    num_line["SNO"]=int( num_line["SNO"])
    num_line["TID"]=int( num_line["TID"])
    num_line["Open FDs"]=int( num_line["Open FDs"])
    num_line["DB Open FDs"]=int( num_line["DB Open FDs"])
    num_line["Flat Log Requests"]=int( num_line["Flat Log Requests"])


    return(num_line)

# --- Enrichment
def enrich_full_sql(datafile):
    # --- Re-initialization ---
    doc_count=0
    DAMDataRec=[]
    field_list=[]
    lineDict={}
    # --- Read next file
    with open(datafile[1]) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for line in csv_reader:
                doc_count += 1
                DAMDataRec.append(line)
    DAMDataRec[0][0]="UTC Offset"
    field_list=DAMDataRec[0]

#    for line in DAMDataRec:
    for i in range(1,len(DAMDataRec)):
        # print ('Processing Rec. Nbr :',i)
        line=DAMDataRec[i]
        lineDict={}
        item_count = 0
        for item in field_list :
            key, values = field_list[item_count],line[item_count]
            lineDict[key] = values
            item_count = item_count + 1

        lineDict['SonarG Source']=datafile[0]
        enriched_line = enrich_line(lineDict)
        enriched_line = num_line(enriched_line)
        # for j in range(0,len(BuffNum)-1):
              # enriched_line[BuffNum[j]]=int(enriched_line[BuffNum[j])
              # print(enriched_line[BuffNum[j]])
              # print(str(BuffNum[j]).strip("[").strip("]"))
              # print(enriched_line[str(BuffNum[j]).strip("[").strip("]")])
              # print(enriched_line["% CPU Sniffer"])
        fullSQLMany.append(enriched_line)
    print("Records processed :",doc_count-1)
    return(doc_count-1)
    
# --- Main  ---
if __name__ == '__main__':
    print("Start BUFF Enrichment")
    os.system('touch ' + path + 'BUFF_Processing_In_Progress')
    ticks_before = time.time()
    # --- Get numeric values
    with open('BUFF_num.txt') as csv_num_file:
        csv_reader = csv.reader(csv_num_file, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for line in csv_reader:
                doc_count += 1
                BuffNum.append(line)

    myclient=Open_Mongo()
    mydb = myclient["AIMAnalytics"]
    mycol = mydb["ENRICHED_BUFF"]
    MetaData(myclient)
    DataFile_List()
    # --- Loop for each DAM data file
    for datafile in DataFiles:
        print('Processing : ',datafile)
        perfDict={}
        perfDict["Processed File"]=datafile
        fullSQLMany=[]
        perfDict["Ticks File Before"] = time.time()
        doc_count=enrich_full_sql(datafile)
        x = mycol.insert_many(fullSQLMany)
        # move the processed file to Processed directory
        shortname=datafile[1][pathlength:]
        # print ("Datafile",datafile," Short name= ",shortname)
        os.rename(datafile[1],pathProcessed + shortname)
        doc_count_total = doc_count_total + doc_count
        perfDict["Total Count Docs"]=doc_count_total
        perfDict["Ticks File After"] = time.time()
        perfDict["Elapsed Time"]=perfDict["Ticks File After"] - perfDict["Ticks File Before"]
        perfDict["Speed of Processing"]=(perfDict["Elapsed Time"] / perfDict["Total Count Docs"])*1000
        perfList.append(perfDict)
    ticks_after = time.time()
    elapsed_time=ticks_after - ticks_before
    print ('Nbr of Docs Processed' , doc_count_total)
    print ('Elapsed time : ', (elapsed_time))
    print ('Speed of Processing : ', (elapsed_time/doc_count_total)*1000)
    # print ('Writing Performances : ')
    mycol = mydb["ENRICHED_BUFF_Perf"] 
    os.system('rm -f ' + path + 'BUFF_Processing_In_Progress')
    x = mycol.insert_many(perfList)
    print("End BUFF Enrichment")
