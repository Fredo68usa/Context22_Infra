$ cat StapsDown_OO.py
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
import csv
import psycopg2
import numpy as np
import pandas as pd


class StapsDown:

  def __init__(self,param_json):
     self.myListMeta_IP = []
     self.myListMeta_Env = []
     self.myListMeta_SubEnv = []
     self.myListMeta_FQDN = []
     self.myListIPs = []
     self.cnt = 0

     #Access to PostGreSQL
     self.postgres_connect = None
     self.cursor = None

     self.CURRENT_TIMESTAMP = None
     self.myListColls = None

     with open(param_json) as f:
          self.param_data = json.load(f)


     self.mongoUserName =  self.param_data["mongoUserName"]
     username = urllib.parse.quote_plus(self.mongoUserName)
     self.mongoPwd =  self.param_data["mongoPwd"]
     password = urllib.parse.quote_plus(self.mongoPwd)
     self.mongoString =  self.param_data["mongoConnectString"]
     self.myclient = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1:27117/admin' % (username, password))

     self.Docs2F = None

  # Opening PostGreSQL
  def open_PostGres(self):

    try:
         self.postgres_connect = psycopg2.connect(user = "sonargd",
                                  # password = "AIM2020",
                                  # host = "127.0.0.1",
                                  port = "5432",
                                  database = "infra"
                                  )


    except (Exception, psycopg2.Error) as error :
         print("Error while connecting to PostgreSQL", error)
         print ("Hello")



  def posGresPrep(self):
      # myclient=open_Mongo()
      # -- Open PostGres
      p1.open_PostGres()
      self.cursor = self.postgres_connect.cursor()
      print ( self.postgres_connect.get_dsn_parameters(),"\n")
      self.cursor.execute("SELECT version();")
      record = self.cursor.fetchone()
      print("You are connected to - ", record,"\n")

      postgres_truncate_query = """ TRUNCATE COLL_STAP2"""
      self.cursor.execute(postgres_truncate_query)
      self.postgres_connect.commit()
      postgres_truncate_query = """ TRUNCATE STAP CASCADE"""
      self.cursor.execute(postgres_truncate_query)
      self.postgres_connect.commit()
      postgres_truncate_query = """ TRUNCATE COLL CASCADE"""
      self.cursor.execute(postgres_truncate_query)
      self.postgres_connect.commit()


  # --- Metadata  ----
  def MetaData(self):
      mydb = self.myclient["sonargd"]

      # STAPs Metadata
      print ('STAPs ')
      mycol = mydb["A_IPs"]
      # self.myListIPs = pd.DataFrame(list(mycol.find({'Retired' : False, 'Physical Type' : 'Node', 'Server Type' : 'Database'})))
      self.myListIPs = pd.DataFrame(list(mycol.find({ 'Physical Type' : 'Node', 'Server Type' : 'Database'})))
      now = datetime.datetime.now()
      # postgres_insert_query = """ INSERT INTO STAP (stapip, stapmetadata, last_update) VALUES (%s,%s,%s) ON CONFLICT (stapip) DO  NOTHING"""
      # postgres_insert_query = """ INSERT INTO STAP (stapip, stapmetadata, last_update, env, retired) VALUES (%s,%s,%s,%s:w) ON CONFLICT (stapip) DO  NOTHING"""
      postgres_insert_query = """ INSERT INTO STAP (stapip, stapmetadata, last_update, env, retired) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (stapip) DO  NOTHING"""
      cnt = 1
      for index, row in self.myListIPs.iterrows():
        cnt = cnt + 1
        record_to_insert = (row['IP'], row['FQDN'] , now , row['Env'], row['Retired'])
        # record_to_insert = (row['IP'], row['FQDN'] , now , row['Env'] ))
        self.cursor.execute(postgres_insert_query, record_to_insert)

      self.postgres_connect.commit()

      #  Collectors Metadata
      mycol = mydb["A_COLLECTORS"]

      myListColls = pd.DataFrame(list(mycol.find({})))
      # print ('myListColls  ', myListColls )
      postgres_insert_query = """ INSERT INTO COLL (coll, env, dbtype)  VALUES (%s,%s,%s) ON CONFLICT (coll) DO  NOTHING"""
      cnt = 1
      for index, row in myListColls.iterrows():
        # print ( ' --' , row['Collectors'], ' -- ')
        record_to_insert = (row['Collectors'], row['Server Env'] , row['DB Type'] )
        self.cursor.execute(postgres_insert_query, record_to_insert)

      self.postgres_connect.commit()


  def getStatusInfo(self):

      # --- Getting STAP infos ----
      mydb = self.myclient["sonargd"]
      mycol = mydb["stap_status"]

      date_start = datetime.datetime.now() - datetime.timedelta(hours=3)
      print (' Start Date : ', date_start )
      # --- Loop for each Critical Server
      Nbr = mycol.find({'Timestamp':{'$gte': date_start}}).count()
      if Nbr == 0 :
          print ('No Records To Process')
          exit(0)

      Docs = pd.DataFrame(list(mycol.find({'Timestamp':{'$gte': date_start}})))
      print ('Nbr of Docs to be processed : ' , Nbr, ' -- ' , len(Docs))

      Docs2 = pd.pivot_table(Docs,values='Last Response Received', index=['TAP IP', 'SonarG Source','Primary Host Name'], aggfunc=max)
      # Flatenning the Pivot Table .. thx God ...
      self.Docs2F = pd.DataFrame(Docs2.to_records())

      now = datetime.datetime.now()
      postgres_insert_query = """ INSERT INTO STAP (stapip, stapmetadata, last_update) VALUES (%s,%s,%s) ON CONFLICT (stapip) DO NOTHING"""
      for index, row in self.Docs2F.iterrows():
           # print ( ' row [0] ' , row[0] )
           # print ( ' ' , row[0] )
           record_to_insert = (row[0],'Not in Inventory', now )
           # print ('Not in Inventory',row[0])
           # print (record_to_insert)
           self.cursor.execute(postgres_insert_query, record_to_insert)
           data = self.myListIPs[self.myListIPs['IP'] == row[0]]
           self.myListMeta_IP.append(data['IP'].values)
           self.myListMeta_FQDN.append(data['FQDN'].values)
           self.myListMeta_Env.append(data['Env'].values)
           self.myListMeta_SubEnv.append(data['Sub Env'].values)

      self.postgres_connect.commit()

  def CollStap(self):
            # Erase the Table first
      postgres_truncate_query = """ TRUNCATE COLL_STAP2"""
      self.cursor.execute(postgres_truncate_query)
      self.postgres_connect.commit()
      # Insert latest Coll Stap info
      # postgres_insert_query = """ INSERT INTO COLL_STAP2 (stapip, collip, last_contact) VALUES (%s,%s,%s)"""
      postgres_insert_query = """ INSERT INTO COLL_STAP2 (stapip, collip, last_contact, primcollip) VALUES (%s,%s,%s,%s)"""
      for index, row in self.Docs2F.iterrows():
           record_to_insert = (row['TAP IP'], row['SonarG Source'], row['Last Response Received'] , row['Primary Host Name'])
           # print("record_to_insert : ", record_to_insert )
           # print ('row[Env]' , row['Env'], ' -- ', row['TAP IP'] )
           # record_to_insert = (row['TAP IP'], row['SonarG Source'], row['Last Response Received'] , row['Env'])
           self.cursor.execute(postgres_insert_query, record_to_insert)

      self.postgres_connect.commit()

      cnt = 0
      for index, row in self.myListIPs.iterrows():
          cnt = cnt + 1
          # print('Ref IP' , row['IP'], ' -- ' , cnt )
          data = self.Docs2F[self.Docs2F['TAP IP'] == row['IP']]
          # data = self.Docs2F[self.Docs2F['TAP IP'] == row['IP'] & row['Retired'] == False]
          if data.empty :
             if  row['Retired'] == False:
                 print ('STAP no Status', row['IP'],' -- ' , row['FQDN'], row['Retired'])

      if(self.postgres_connect):
         self.cursor.close()
         self.postgres_connect.close()
         print("PostgreSQL connection is closed")


      print("End Detection of STAPs down")


if __name__ == '__main__':
      print("Start Detection of STAPs down")
      # print ('Type of myListIPs', type(myListIPs))
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  #Create a TCP/IP
      p1 = StapsDown("param_data.json")

      p1.posGresPrep()

      p1.MetaData()

      p1.getStatusInfo()

      p1.CollStap()

(env)
sonargd@sdcpiapplnx143 NoTraffic_OO
$
