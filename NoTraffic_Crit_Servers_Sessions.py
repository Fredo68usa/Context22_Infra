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

myListIPsCrit = []

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
    mycol = mydb["A_IPs_Crit"]
    for row in mycol.find():
            myListIPsCrit.append(row)



if __name__ == '__main__':
    print("Start Detection of No Traffic (sessions) on Critical Servers")
    # -- Read list of Crit Servers
    myclient=Open_Mongo()
    MetaData(myclient)
    # -- Set up to read Collection
    mydb = myclient["sonargd"]
    mycol = mydb["session"]

    # date_start = datetime.datetime(2020, 8,10, 5)
    date_start = datetime.datetime.now() - datetime.timedelta(hours=10)
    print("Starting Date : ", date_start)
    # --- Loop for each Critical Server
    for critServ in myListIPsCrit:
        # ---- Reading of the Collection
        Nbr = mycol.find({'Server IP':critServ['IP'], 'Session Start':{'$gte': date_start}}).count()
        # Nbr = mycol.find({'Server IP':critServ['IP'], 'Timestamp':{'$gte': date_start}}).count()
        # print ("Hello",critServ['IP'], " -- ", Nbr)
        if Nbr == 0:
           print ("No Traffic on ",' -- Tier ', critServ['Tier'],"--",critServ['DB Type'], ' -- ', critServ['Env'], '-- ', critServ['Sub Env'],'-- ', critServ['IP']," - ", critServ['FQDN'] )
