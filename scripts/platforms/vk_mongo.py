import pymongo, os, pickle , sys
import datetime
from bson.objectid import ObjectId
#6246efa9fe154f8c21023c35

client = pymongo.MongoClient("mongodb://localhost:27017/")
#db = client.test_database
db = client["original"]
collection = db['vk']
adb = client["archive"]
acollection = adb['vk']

def getLink(row):
    if row['post_type'] == 'reply':
        return 'https://vk.com/wall-%s_%s?%s'  % (str(row['owner_id']).replace('-',''), str(row['post_id']), str(row['id']))
    elif row['post_type'] == 'post':
        return 'https://vk.com/wall-%s_%s' % (str(row['owner_id']).replace('-',''), str(row['id']))

#print(dir(collection))
#import datetime
#post = {"author": "Mike",
#        "text": "My first blog post!",
#        "tags": ["mongodb", "python", "pymongo"],
#        "date": datetime.datetime.utcnow()}
print(db.list_collection_names())
#post= collection.find_one()
#collection.delete_one({'_id': ObjectId('6246efa9fe154f8c21023c35')})
#print(post)
#posts = collection.posts
#posts = db.posts
#pid = collection.insert_one(post).inserted_id
#print(pid)
#print(client.list_database_names())

result = collection.create_index([('link', pymongo.ASCENDING)], unique=True)
known={}
for post in collection.find():
    known[post['link']] = 1
for post in acollection.find():
    known[post['link']] = 1
abi = os.listdir('../../datasets/general_queries_got')
for i, x in enumerate(abi):#os.listdir('../../datasets/general_queries_got'):
    #print(x)
    kw, dt = x.split('_')
    dt = dt[:-2]
    dt = str(datetime.datetime.strptime(dt, "%Y-%m-%d").date())
    #test = collection.find_one({'keyword':kw, 'querydate': dt})
    #print(test)
    #if not test is None:
    #    print("Done")
    #    continue
    dta = pickle.load(open('../../datasets/general_queries_got/%s'% x,'rb'))
    to_add=[]
    for xx in dta:
        link = getLink(xx)
        try:
            known[link]#avoid duplicates
        except KeyError:
            known[link]=1
            xx['keyword'] = kw
            xx['querydate'] = dt
            xx['link'] = link
            to_add.append(xx)
    if not to_add == []:
        collection.insert_many(to_add)
    print("%s/%s"% (i, len(abi)),x,len(to_add))
