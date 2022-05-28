import psycopg2
import sys
import csv

csv.field_size_limit(sys.maxsize)

conn=psycopg2.connect(user="", database="", password="",host='',port='')
conn.set_client_encoding('UTF8')
cur = conn.cursor()

import codecs, csv
cur.execute("select url from urls")
row=cur.fetchone()
kn={}
while row:
    kn[row[0]] = 1
    row=cur.fetchone()
ct=0
with codecs.open('urls.csv','r', encoding='utf-8') as f:
    r=csv.reader(f, delimiter=';')
    for row in r:
        ct+=1
        if len(row) > 2:
            try:
                kn[row[0]]
            except KeyError:
                if row[1] == '':
                    row[1] = None
                if row[2] == '':
                    row[2] = None
                cur.execute("insert into urls (url, resolve, tld) values (%(u)s, %(r)s, %(t)s) on conflict do nothing",{
                    'u': row[0],
                    't': row[2],
                    'r': row[1]

                })
        if ct % 1000 == True:
            print(ct)
            conn.commit()
conn.commit()
