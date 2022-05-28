import pymongo, os, pickle , sys
import shutil
import json,codecs, csv
import datetime
from bson.objectid import ObjectId
#6246efa9fe154f8c21023c35

client = pymongo.MongoClient("mongodb://localhost:27017/")
#db = client.test_database
db = client["original"]
collection = db['te']
archive = client['archive']
archcoll = archive['te']

#print(dir(collection))
#import datetime
print(db.list_collection_names())
post= collection.find_one()
#collection.delete_one({'_id': ObjectId('6246efa9fe154f8c21023c35')})
#posts = collection.posts
#posts = db.posts
#pid = collection.insert_one(post).inserted_id
#print(pid)
#print(client.list_database_names())

def findDelimiter(tarpath):
    delim='\t'
    with codecs.open(tarpath,'r', encoding='utf-8') as f:
        for l in f:
            if l.count(';') > l.count('\t'):
                delim=';'
            if l.count(',') > l.count(';') and l.count(',') > l.count('\t'):
                delim=','
            break
    return delim

def getLink(row):
    try:
        return 'https://t.me/%s/%s' % (row['search_entity'], row['id'])
    except KeyError:
        return 'https://t.me/%s/%s' % (row['author_username'], row['id'])

result = collection.create_index([('link', pymongo.ASCENDING)], unique=True)

known={}
for post in collection.find():
    known[post['link']] = 1
for post in archcoll.find():
    known[post['link']] = 1
ddir = '/home/emillie/uploads'
for x in os.listdir(ddir):
    to_add=[]
    added=0
    if not x.endswith('.csv'):
        continue
    if 'Telegram' in x or ('Channels' in x and 'Twitter' in x):
        delim = findDelimiter('%s/%s'%(ddir,x))
        #print([x,delim])
        #shutil.copyfile('../../datasets/%s' %x,'tmp.csv')
        #q=open('tmp.csv','r', encoding='utf-8').read().replace(chr(8232),' ').replace(chr(8233),'')
        #with codecs.open('tmp.csv','w', encoding='utf-8') as f:
        #    f.write(q)
        with codecs.open('%s/%s'%(ddir,x),'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=delim)
            for xx in r:

                link = getLink(xx)
                try:
                    known[link]#avoid duplicates
                except KeyError:
                    added+=1
                    known[link]=1
                    xx['sourcefile'] = x
                    xx['link'] = link
                    to_add.append(xx)
                    if len(to_add) > 100:
                        collection.insert_many(to_add)
                        to_add=[]
        #os.unlink('tmp.csv')
        #if not to_add == []:
        print(added)
        if len(to_add) > 0:
            collection.insert_many(to_add)
        shutil.move('%s/%s'%(ddir, x), '%s/processed/%s'%(ddir,x))
