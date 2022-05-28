import pymongo, os, pickle , sys
import shutil
import json,codecs, csv
import datetime
from bson.objectid import ObjectId
#6246efa9fe154f8c21023c35
sys.exit()
client = pymongo.MongoClient("mongodb://localhost:27017/")


#db = client.test_database
db = client["original"]
collection = db['tw']
adb = client["archive"]
acollection = adb['tw']

deleteq=False
if deleteq:
    dba = client['queries']
    tc = dba['twqueries']
    for z in tc.find():
        tc.delete_one({'_id': z['_id']})
        print(z)
    #for x in collection.find({'sourcefile': {'$exists': False}}):
    #    print(x)
    #print(tc.count_documents({}))
    sys.exit()

#print(dir(collection))
#import datetime
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

def getId( row):
    try:
        return row['id_str']
    except KeyError:
        return str(row['id'])
def getLink(row):
    lid=getId(row)
    try:
        int(lid)
    except Exception:
        print(row)
        sys.exit()
    return 'https://twitter.com/a/status/%s'% lid

result = collection.create_index([('link', pymongo.ASCENDING)], unique=True)

start=True
known={}
ba=[]
ct=0
import pickle
rr=0
for post in collection.find({'inserted': {'$exists':False}}):
    ba.append(post['id'])
    if len(ba) >= 100000:
        rr+=1
        pickle.dump(ba, open('tw%s.p'%rr,'wb'))
        print(rr)
        ba=[]
    #    break
    ct+=1
rr+=1
pickle.dump(ba, open('tw%s.p'%rr,'wb'))
print(ct/1000)
sys.exit()
for post in collection.find():
    known[post['link']] = 1
for post in acollection.find():
    known[post['link']] = 1
for x in os.listdir('../../datasets'):
    to_add=[]
    added=0
    if 'Twitter - RU - General - 1-1-2022 to 9-3' in x:
        start=True
    if not start:
        continue
    if 'Twitter' in x and x.endswith('.csv') and not 'Channels' in x:
        delim = findDelimiter('../../datasets/%s'%x)
        print([x,delim])
        tf = '../../datasets/%s'%x
        if True:
            tf='tmp.csv'
            #print("Copy...")
            #shutil.copyfile('../../datasets/%s' %x,'tmp.csv')
            with codecs.open('tmp.csv','w', encoding='utf-8') as wf:
                with open('../../datasets/%s'%x,'r', encoding='utf-8') as f:
                    for l in f:
                        wf.write("%s\n"%l.replace(chr(8232),' ').replace(chr(8233),''))
        with codecs.open(tf,'r', encoding='utf-8') as f:
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
        os.unlink('tmp.csv')
        #if not to_add == []:
        #    collection.insert_many(to_add)
    elif 'Twitter' in x and x.endswith('.ndjson'):
        with codecs.open('../../datasets/%s'%x, 'r', encoding='utf-8') as f:
            for l in f:
                if not l.strip():
                    continue
            row=json.loads(l)
            link = getLink(row)
            try:
                known[link]
            except KeyError:
                added+=1
                known[link] = 1
                row['sourcefile'] = x
                row['link'] = link
                to_add.append(row)
                if len(to_add) > 100:
                    collection.insert_many(to_add)
                    to_add=[]
    print(added)
    if len(to_add) > 0:
        collection.insert_many(to_add)
