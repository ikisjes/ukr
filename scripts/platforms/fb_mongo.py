import pymongo, os, pickle , sys
import shutil
import json,codecs, csv
import datetime
from bson.objectid import ObjectId


client = pymongo.MongoClient("mongodb://localhost:27017/")
#db = client.test_database
db = client["original"]
collection = db['fb']
adb = client["archive"]

acollection = adb['fb']
#print(dir(collection))
#import datetime
#print(db.list_collection_names())
#post= collection.find_one()
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
    if row['URL'] in (None, ''):
        if row['Link'] in (None, ''):
            try:
                fakelink = 'NO_LINK_FOR' + row['Facebook Id'] + row['\ufeffGroup Name'] + row['Message'] + row['Post Created Time'] + row['Post Created Date']
            except KeyError:
                fakelink = 'NO_LINK_FOR' + row['Facebook Id'] + row['\ufeffPage Name'] + row['Message'] + row['Post Created Time'] + row['Post Created Date']
            return fakelink
        if len(row['Link']) > 10:
            return row['Link']
        
    if len(row['URL']) < 10:
        print(row)
        print("SMALL URL", row['URL'])
    assert len(row['URL']) >  10
    assert 'facebook'  in row['URL']
    return row['URL']

#result = collection.create_index([('link', pymongo.ASCENDING)], unique=True)

known={}
for post in collection.find():
    known[post['link']] = 1
for post in acollection.find():
    known[post['link']] = 1
srcfolder = '/home/emillie/uploads'
for x in os.listdir(srcfolder):
    to_add=[]
    added=0
    try:
        os.unlink('tmp.csv')
    except Exception:
        pass
    if not x.endswith('.csv'):
        continue
    if 'Facebook' in x or 'FB -' in x:
        print(x)
        delim = findDelimiter('%s/%s'%(srcfolder,x))
        #print([x,delim])
        shutil.copyfile('%s/%s' %(srcfolder ,x),'tmp.csv')
        q=open('tmp.csv','r', encoding='utf-8').read().replace(chr(8232),' ').replace(chr(8233),'')
        with codecs.open('tmp.csv','w', encoding='utf-8') as f:
            f.write(q)
        with codecs.open('tmp.csv','r', encoding='utf-8') as f:
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
        shutil.move('%s/%s'% (srcfolder, x), '%s/processed/%s' % (srcfolder, x))
