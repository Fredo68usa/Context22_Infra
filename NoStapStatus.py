import pymongo
import urllib.parse
import json
import getpass
import datetime
import time
import os
import calendar
import socket
import glob

myListCollectors = []

# --- Opening Mongo
def Open_Mongo():

   # --- Password ----
   try:
        p = getpass.getpass()
   except Exception as error:
        print('ERROR', error)
   else:
        print('Password entered')

   # ----- Connection
   username = urllib.parse.quote_plus('fpetit')
   password = urllib.parse.quote_plus(p)

   myclient = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1:27117/admin' % (username,password))
   return(myclient)

# --- Metadata  ----
def MetaData(myclient):
    mydb = myclient["sonargd"]
    mycol = mydb["A_COLLECTORS"]
    for row in mycol.find():
            myListCollectors.append(row)



if __name__ == '__main__':
    print("Start Detection of No Traffic on Collectors")
    # -- Read list of Crit Servers
    myclient=Open_Mongo()
    MetaData(myclient)
    # -- Set up to read Collection
    mydb = myclient["sonargd"]
    mycol = mydb["stap_status"]

    # date_start = datetime.datetime(2020, 8,28,5)
    date_start = datetime.datetime.now() - datetime.timedelta(hours=10)
    print("State of Stap Status from : ", date_start)
    # --- Loop for each Critical Server
    for Collectors in myListCollectors:
        # print('Processing : ',critServ['IP'])
        # ---- Reading of the Collection
        Nbr = mycol.find({'SonarG Source':Collectors['Collectors'], 'Timestamp':{'$gte': date_start}}).count()
        # print ("Hello",critServ['IP'], " -- ", Nbr)
        if Nbr == 0:
           print ("No Stap Status on ",Collectors['Collectors'])
