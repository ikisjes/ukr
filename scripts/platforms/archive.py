import pymongo
import psycopg2
from te import Telegramy
from vk import Vky
from fb import Facebooky
from tw import Twittery
from random import randint

#cur = conn.cursor()
def getLinks():
    us={}
    conn=psycopg2.connect(database="", user="", password="", host='',port='')
    conn.set_client_encoding('UTF8')
    with conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
        cursor.itersize = 10000
        cursor.execute("select link from posts")
        for row in cursor:
            us[row[0]]=1
    return us
us = getLinks()
client=pymongo.MongoClient('mongodb://localhost:27017')
db = client['original']
pf='te'
obmap = {
    'te': Telegramy(),
    'vk': Vky(),
    'fb': Facebooky(),
    'tw': Twittery()
}

#for pf in ['te','tw','vk','fb']:
for pf in ['fb','te','vk','tw']:
    collection = db[pf]
    db2 = client['archive']
    collection2 = db2[pf]

    ob = obmap[pf]
    print(pf)
    ct=0
    for x in collection.find({'inserted': True}).limit(100000):
        ct+=1
        if ct % 1000 == True:
            print(ct, pf)
        _id = x['_id']
        del x['_id']
        del x['inserted']
        link = ob.getLink(x)
        try:
            us[link]
            collection2.insert_one(x)
            collection.delete_one({'_id': _id})
        except KeyError as err:
            print('KeyError', err)
