import sys
sys.path.append('/var/scripts')
from random import randint
import traceback
from functions import read_keywords
import psycopg2
import langid, re
import os
from random import shuffle
import pickle
import pymongo
from datetime import datetime


class abstractPlatform:
    DATASETDIR='Datasets-20220302T134933Z-001/Datasets'
    cur=None
    cur2=None
    case_sensitive = ['ООН','НАТО','EC','ЛНР','ДНР','SWIFT','ООН','НАТО','ЄС']
    platform=None
    kw={}
    collection = None
    knownurls={}
    def __init__(self):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        db = client['original']
        self.collection = db[self.platform]
    def getId(self,row):
        raise NotImplementedError

    def connect(self):
        if not self.cur:
            self.conn=psycopg2.connect(database="", user="", password="",host='',port='')
            self.conn.set_client_encoding('UTF8')
            self.cur = self.conn.cursor()
    def connect2(self):
        if not self.cur2:
            self.conn2=psycopg2.connect(database="", user="", password="",host='',port='')
            self.conn2.set_client_encoding('UTF8')
            self.cur2 = self.conn.cursor()


    def findDelimiter(self,tarpath):
        delim='\t'
        with codecs.open(tarpath,'r', encoding='utf-8') as f:
            for l in f:
                if l.count(';') > l.count('\t'):
                    delim=';'
                if l.count(',') > l.count(';') and l.count(',') > l.count('\t'):
                    delim=','
                break
        return delim

    def read_keywords(self):
        self.kw = read_keywords()
    
    def getLanguage(self, row):
        text = self.getText(row)
        lan = None
        try:
            lan = langid.classify(text)[0]
        except Exception as err:
            pass
        return lan
    
    def getHashtags(self, row):
        try:
            try:
                if row['hashtags'] is None:
                    return []
                return [a.lower() for a in row['hashtags'].split(',') if a.strip()]
            except KeyError:
                tags=[]
                for x in self.tokenize(row):
                    if x.lower().startswith('#') and len(x) > 1:
                        tags.append(x.lower())
                return tags
        except TypeError as er:
            print(er)
            tags=[]
            for x in self.tokenize(row):
                if x.lower().startswith('#') and len(x) > 1:
                    tags.append(x.lower())
            return tags
    def getLocus(self, row):
        lang=None
        place=None
        try:
            z = row['locus'].split('place_country:')
            lang = row['locus'].replace('lang:','').split(' ')
            if len(z) == 2:
                place = z[-1]#.replace('lang:','')
                
        except KeyError:
            pass
        return (lang, place)
    def getUrls(self, row):
        try:
            if row['urls'] is None:
                return []
            return row['urls'].replace("\n",',').split(',')
        except KeyError:
            urls=[]
            txt=self.getText(row)
            if not txt is None:
                for x in txt.replace('\n',' ').split(' '):
                    if x.lower().startswith('http'):
                        urls.append(x)
            return urls  
    def getText(self, row):
        try:
            return row['text']
        except KeyError:
            return row['body']

    def validateRow(self, row):
        if not row['engagement'] is None:
            float(row['engagement'])
        #assert not row['username'].isnumeric()
        assert len(row['language']) == 2
        assert len(row['platform']) == 2
        #if not type(row['date']) == datetime.date:
        #    print(type(row['date']))
        #    assert re.match('^202[0-9]+\-[0-9]{2}\-[0-9]{2}$',row['date'])
        assert row['link'].startswith('http')

    def __iter__(self):
        ct=0
        self.connect()
        cachi={}
        print("Loading known...", self.platform)
        self.cur.execute("select platformid, lid from posts where platform = %(p)s", {'p': self.platform})
        while True:
            rows = self.cur.fetchmany(10000)
            if len(rows) > 0:
                for row in rows:
                    cachi[row[0]] = row[1]
            else:
                break
        print("Loaded!")
        runner=0

        #runner=1#3
        #qqra={}
        #for x in os.listdir('.'):
        #    if x.startswith('tw'):
        #        try:
        #             num = int(x.replace('tw','').replace('.p',''))
        #             print(num)
        #             if (num + runner) % 3 ==True:
        #                print('yay')
        #                qra = set(pickle.load(open(x,'rb')))
        #                for z in qra:
        #                    qqra[z]=1
        #        except ValueError as er:
        #            print(er)
        #print(len(qqra))
        #sys.exit()
        for row in self.collection.find({
                    'inserted': {'$exists': False}
                }):
            ct+=1
            if ct == 1:
                print("Mongo query started")
            if ct % 10000 == True:
                print(ct)
            #try:
            #    qqra[row['id']]
            #except KeyError:
            #    continue
            #if not row['id'] in qra:
                #continue
            #if ct % 1000 == True:
            #    print(ct, self.platform)
            if self.platform == 'tw':
                if not row['author_id'].isnumeric():
                    #anonymized files, we don't want
                    continue
            pfid = self.getId(row)
            try:
                lid=cachi[pfid]
            except KeyError:
                lid=None
            language = self.getLanguage(row)
            text = self.getText(row)
            try:
                usr = self.getUser(row)
            except Exception as err:
                print(err)
                continue
            kws = ','.join(self.getKeywordsFromText(row))
            if kws == '':
                kws=None
            urls = ','.join(self.getUrls(row))
            if urls == '':
                urls=None
            tags = ','.join(self.getHashtags(row))
            if tags == '':
                tags=None
            eng = self.getEngagement(row)
            if eng == '' or eng is None:
                eng = None
            else:
                eng = float(eng)
            userid = self.getUserId(row)
            loclang, locus = self.getLocus(row)
            yield {
                'lid': lid,
                #'filename': x,
                'mongoid': row['_id'],
                'username': usr,
                'language': language,
                'text': text,
                'platformid': pfid,
                'platform': self.platform,
                'date': self.getDate(row),
                'engagement': eng,
                'hashtags': tags,
                'urls': urls,
                'keywords': kws,
                'link': self.getLink(row),
                'userid': userid,
                'locus': locus,
                'loclang': loclang
            }

    def __OLDiter__(self):
        self.connect()
        cachi={}
        print("Loading known...")
        self.cur.execute("select platformid, lid from posts where platform = %(p)s", {'p': self.platform})
        while True:
            rows = self.cur.fetchmany(10000)
            if len(rows) > 0:
                for row in rows:
                    cachi[row[0]] = row[1]
            else:
                break
        print("Loaded!")
        q =  os.listdir(self.DATASETDIR)
        # from random import shuffle
        shuffle(q)
        for ija, x in enumerate(q):
            if self.isValidFile(x):
                print(x, ija, len(q))
                #try:
                #    if x in self.already_processed[self.platform]:
                #        continue
                #except KeyError:
                #    pass
                tarpath=os.path.join(self.DATASETDIR, x)
                dta = []
                if x.endswith('.p'):
                    #if ija < 3956:
                    #    continue
                    #if not '2022-03-' in x:
                    #    continue
                    #if not x in self.newfiles:
                    #    continue
                    
                    dta = pickle.load(open(tarpath,'rb'))
                elif x.endswith('.ndjson'):
                    with codecs.open(tarpath, 'r', encoding='utf-8') as f:
                        for l in f:
                            if not l.strip():
                                continue
                            rct+=1
                            if rct % 1000 == True:
                                print(rct/1000)
                            dta.append(json.loads(l))
                else:
                    delim = self.findDelimiter(tarpath)
                    with codecs.open(tarpath,'r', encoding='utf-8') as f:
                        r=csv.DictReader(f, delimiter=delim)
                        for row in r:
                            dta.append(row)
                for i, row in enumerate(dta):
                    pfid = self.getId(row)
                    try:
                        lid=cachi[pfid]
                    except KeyError:
                        lid=None
                    language = self.getLanguage(row)
                    text = self.getText(row)
                    usr = self.getUser(row)
                    kws = ','.join(self.getKeywordsFromText(row))
                    if kws == '':
                        kws=None
                    urls = ','.join(self.getUrls(row))
                    if urls == '':
                        urls=None
                    tags = ','.join(self.getHashtags(row))
                    if tags == '':
                        tags=None
                    eng = self.getEngagement(row)
                    eng = float(eng)
                    userid = self.getUserId(row)
                    loclang, locus = self.getLocus(row)
                    yield {
                        'lid': lid,
                        #'filename': x,
                        'username': usr,
                        'language': language,
                        'text': text,
                        'platformid': pfid,
                        'platform': self.platform,
                        'date': self.getDate(row),
                        'engagement': eng,
                        'hashtags': tags,
                        'urls': urls,
                        'keywords': kws,
                        'link': self.getLink(row),
                        'userid': userid,
                        'locus': locus,
                        'loclang': loclang
                    }


    def getKeywordsFromText(self, row):
        if self.kw == {}:
            self.read_keywords()
        txt=self.getText(row)
                # from nltk import TweetTokenizer

        # tokki=TweetTokenizer()
        # kws=[]
        # if not txt is None:
            # try:
                # self.tt
            # except Exception:
                # from nltk import TweetTokenizer
                # self.tt = TweetTokenizer()
            # txt = self.tt.tokenize(txt.replace('#',' ').lower())


            # for k,v in self.kw.items():
                # for kk,words in v.items():
                    # for w in words:
                        # stance = '%s: %s'% (k,kk)
                        # if w.endswith('*'):
                            # numbers=0
                            # w=w[:-1]
                            # for qq in txt:
                                # if qq.startswith(w):
                                    # numbers+=1
                                    # kws.append(w)
                        # else:
                            # numbers = txt.count(w)
                            # for i in range(0,numbers):
                                # kws.append(w)

        # return kws
        # print(self.case_insensitive_keywords)
        kws=[]
        for k,v in self.kw.items():
            for kk, vv in v.items():
                kws+=vv
        try:
            kws.remove('мир')
        except Exception:
            pass
        try:
            self.tokki
        except Exception:
            from nltk import TweetTokenizer
            self.tokki=TweetTokenizer()

        toks=self.tokki.tokenize(txt)
        founds=[]
        for t in toks:
            if t.startswith('#'):
                t=t[1:]
            for q in kws:
                comp='sensitive'
                if q in self.case_sensitive:
                    if q.endswith('*'):
                        if t.startswith(q[:-1]):
                            founds.append(q)
                    else:
                        if t == q:
                            founds.append(q)
                else:
                    if q.endswith('*'):
                        if t.lower().startswith(q.lower()[:-1]):
                            founds.append(q)
                    else:
                        if t.lower() == q.lower():
                            founds.append(q)

        return founds


    def tokenize(self, row):
        try:
            self.tt
        except Exception:
            from nltk import TweetTokenizer
            self.tt = TweetTokenizer()
        try:
            return self.tt.tokenize(self.getText(row))
        except Exception as er:
            print(er)
            print(type(self.getText(row)))
            return []

    def dotheinsert(self, dic):
        if dic['language'] == 'und':
            dic['language']  = self.getLanguage(dic)
        self.validateRow(dic)
        if dic['userid'] == '':
            dic['userid'] = None
        if ',' in str(dic['engagement']):
            dic['engagement']=float(dic['engagement'].replace(',','.'))
        if dic['engagement'] == '':
            dic['engagement']=None
        if dic['date'] is None:
            return
        try:
            self.cur.execute("insert into posts (platformid, platform, text, username, language, engagement, date, urls, keywords, hashtags, link, userid, locus, loclang) VALUES (%(platformid)s, %(platform)s, %(text)s, %(username)s, %(language)s, %(engagement)s, %(date)s, %(urls)s, %(keywords)s, %(hashtags)s, %(link)s, %(userid)s, %(locus)s, %(loclang)s)", dic)
            self.insertUrls(dic)
        except Exception as err:
            self.conn.rollback()
            print(traceback.format_exc())
            print("Insert data err")
            print(self.platform)
            print(dic)
            sys.exit()

    def insertUrls(self, dic):
        if dic['urls'] is None:
            return
        if not self.cur2:
            self.connect2()
        for u in dic['urls'].split(','):
            u = u[:2700]#too big as pkey otherwise
            if u and u.strip():
                try:
                    self.knownurls[u]
                except KeyError:
                    self.cur2.execute("select url from urls where url like %(u)s",{'u':u})
                    row = self.cur2.fetchone()
                    if not row:
                        self.cur2.execute("insert into urls (url) values (%(u)s)", {'u':u})
                        self.conn2.commit()
                self.knownurls[u]=1

    def checkUserIds(self):
        ct=0
        mids=[]
        b=[]
        total_docs = self.collection.count_documents({'inserted': {'$exists': False}})/1000
        print(total_docs, "to parse")
        for dic in self.collection.find({'inserted':{'$exists':False}}):
            ct+=1
            if ct % 100000 == True:
                print(ct/1000, total_docs, len(b))
            try:
                if not dic['author_id'].isnumeric():
                    b.append(self.getId(dic))
            except KeyError:
                print(dic)
                print(dic.keys())
                sys.exit('Field nf')
        print(len(b),'bad ones')
        pickle.dump(b, open('anonym_ids.p','wb'))
    def insertData(self):
        assert self.platform is not None
        self.connect()
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            cursor.execute("select url from urls")
            for row in cursor:
                self.knownurls[row[0]]=1

        ct=0
        cct=0
        mids=[]
        total_docs = self.collection.count_documents({'inserted': {'$exists': False}})/1000
        print(total_docs, "to parse")
        for dic in self:
            ct+=1
            ok=True
            try:
                if dic['inserted'] == True:
                    mids.append(dic['mongoid'])
                    ok=False
            except KeyError:
                pass
            if ok and dic['lid'] is None:
                try:
                    self.dotheinsert(dic)
                    mids.append(dic['mongoid'])
                    cct+=1
                except Exception as err:
                    print(err)
                    self.conn.rollback()
            elif not dic['lid'] is None:
                mids.append(dic['mongoid'])
            else:
                print('??')
            #try:
            #    self.already_processed[self.platform].append(dic['filename'])
            #except KeyError:
            #    self.already_processed[self.platform] = [dic['filename']]
            if ct % 1000 == True:
                print(self.platform, "%s/%s"% (ct/1000, total_docs))
                self.conn.commit()
                for mid in mids:
                    self.collection.find_one_and_update({"_id": mid}, {"$set": {"inserted": True}})
                mids=[]
        self.conn.commit()
        for mid in mids:
            self.collection.find_one_and_update({"_id": mid}, {"$set": {"inserted": True}})
