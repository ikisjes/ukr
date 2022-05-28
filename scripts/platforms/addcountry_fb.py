import pymongo, os, pickle , sys
import shutil
import json,codecs, csv
import datetime
from bson.objectid import ObjectId
import psycopg2

conn=psycopg2.connect(database='', user='', password="", host='', port='')
cur=conn.cursor()

client = pymongo.MongoClient("mongodb://localhost:27017/")
#db = client.test_database
db = client["original"]
collection = db['fb']
adb = client["archive"]

acollection = adb['fb']
ct=0
uct=0
for post in acollection.find():
    ct+=1
    c=post['Page Admin Top Country']
    if c.strip():
        cur.execute("update posts set locus = %(loc)s where platform = 'fb' and  link = %(link)s",
                {'loc': c, 'link': post['link']})
        conn.commit()
        uct+=1
    if ct % 1000==True:
        print(ct, uct)
