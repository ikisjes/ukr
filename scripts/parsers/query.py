import os,pickle, codecs, csv, psycopg2, time
from datetime import date, datetime, timedelta
import tld
import numpy as np
from itertools import combinations, zip_longest
from collections import Counter
from random import shuffle, randint
import networkx as nx
from operator import itemgetter
from datetime import datetime
from nltk import TweetTokenizer
from string import punctuation

class Query:
    kw={}
    tokenizer=TweetTokenizer()
    top=None
    filters={}
    cur2=None
    cur=None
    seppy="@^#*@&@^#&@*@&@"
    case_sensitive = []# ['ООН','НАТО','EC','ЛНР','ДНР','SWIFT','ООН','НАТО','ЄС']
    outdir='/home/emillie/files'
    def __init__(self):
        if not os.path.isdir(self.outdir):
            os.makedirs(self.outdir)
        if not os.path.isdir(self.outdir+'/rankflows'):
            os.makedirs(self.outdir+'/rankflows')
    
    def connect(self):
        if not self.cur:
            self.conn=psycopg2.connect(database="ukraine", user="dmanagy", password="bcyasbCBStc6@@dcdc",port=5432,host='127.0.0.1')
            self.conn.set_client_encoding('UTF8')
            self.cur = self.conn.cursor()


    def connect2(self):
        if not self.cur2:
            self.conn2=psycopg2.connect(database="ukraine", user="dmanagy", password="bcyasbCBStc6@@dcdc",host='127.0.0.1',port=5432)
            self.conn2.set_client_encoding('UTF8')
            self.cur2 = self.conn2.cursor()


    def read_keywords(self):
        with codecs.open('/var/scripts/keywords.csv','r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter='\t')
            for row in r:
                if row['Keyword (please watch out for stemming *)'].strip():
                    try:
                        self.kw[row['Theme']]
                    except KeyError:
                        self.kw[row['Theme']]={}
                    try:
                        self.kw[row['Theme']][row['Language']]
                    except KeyError:
                        self.kw[row['Theme']][row['Language']]=[]
                    self.kw[row['Theme']][row['Language']].append(row['Keyword (please watch out for stemming *)'].strip().replace('#',''))



    def torelative(self):
        wa={}
        phile = '%s/date_topic_language_platform_None_ in ru-uk.csv'%self.outdir
        with codecs.open(phile,'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                try:
                    wa[row['date']]
                except KeyError:
                    wa[row['date']]={}
                k = "%s_%s_%s" % (row['language'], row['platform'], row['topic'])
                try:
                    wa[row['date']][k]+= int(row['post_count'])
                except KeyError:
                    wa[row['date']][k]=int(row['post_count'])
        out = [['date','language','platform','topic','count','relative_postcount_per_day']]
        for k,v in wa.items():
            tot = np.sum(list(v.values()))
            for kk, vv in v.items():
                pv=(vv/tot)*100
                out.append([k]+kk.split('_')+[vv,pv])
        with codecs.open('%s/date_topic_language_platform_None_ in ru-uk_rel.csv' % self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)
    def apply_filters(self, q):
        if len(self.filters) > 0 and not 'where' in q:
            q += ' where '
        for filter, val in self.filters.items():
            if type(val) == list:
                if not q.strip().endswith('where'):
                    q += " and %s in (%s)"% (filter, "'"+"','".join(val) + "'")
                else:
                    q += " %s in (%s)"% (filter, "'"+"','".join(val) + "'")
            else:
                if not q.strip().endswith('where'):
                    q += " and %s = '%s'"% (filter, val)
                else:
                    q += " %s = '%s'"% (filter, val)
        if 'topic' in q:
            q=q.replace('topic','keywords')
        if 'tlds' in q:
            q=q.replace('tlds','urls')
        return q

    def test(self, a,b,c):
        print([a,b,c])


    def get_basestats(self):
        self.connect()
        self.cur.execute("select language, platform, count(*) from posts group by language, platform")
        row = self.cur.fetchone()
        m={}
        while row:
            l = row[0]
            p=row[1]
            try:
                m[p]
            except KeyError:
                m[p]={}
            m[p][l]=row[2]
            row = self.cur.fetchone()

        per_platform={}
        per_language={}
        out=[]
        for p,v in m.items():

            for l, vv in v.items():
                out.append([p,l,vv])
                try:
                    per_platform[p] += vv
                except KeyError:
                    per_platform[p] = vv
                try:
                    per_language[l] += vv
                except KeyError:
                    per_language[l] = vv
        pp = []
        for k, v in per_platform.items():
            pp.append([k,v])
        pl=[]
        for k, v in per_language.items():
            pl.append([k,v])

        with codecs.open('%s/counts_per_platform.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(pp)
        with codecs.open('%s/counts_per_language.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(pl)
        with codecs.open('%s/counts_per_language_and_platform.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)


    def MediaDietTest(self, pf = []):
        self.connect()
        print('start')
        #urls = pickle.load(open('allurls.p','rb'))
        self.connect()
        self.cur.execute("select url, resolve, tld from urls where resolve is not null and resolve like '%.%'")
        urls={}
        resolutions={}
        tldmap={}
        print('exec')

        ct=0
        while True:
            rows = self.cur.fetchmany(10000)

            if len(rows) > 0:
                for row in rows:
                    ct+=1
                    # if ct % 10000 == True:
                        # print(ct)
                    if row[1]:
                        resolutions[row[0]] = row[1]
                        resolutions[row[1]] = row[1]
                    if row[2]:
                        tldmap[row[0]] = row[2]
                        tldmap[row[1]] = row[2]
                    # urls[row[0]] = [row[1], row[2]]
            else:
                break
        self.cur.execute("select url, resolve, tld from urls where tld is not null and resolve not like '%.%'")
        while True:
            rows = self.cur.fetchmany(10000)

            if len(rows) > 0:
                for row in rows:
                    ct+=1
                    if ct % 10000 == True:
                        print(ct)
                    tldmap[row[0]] = row[2]
                    tldmap[row[1]] = row[2]
                    # urls[row[0]] = [row[1], row[2]]
            else:
                break

        if pf == []:
            pf = ['te','fb','vk','tw']
        unresolved=[]
        untldd=[]
        numericusers=[]
        for platform in pf:
            self.cur.execute("select text, username, lid from posts where language='ru' and   platform = '"+platform+"'")
            ct=0
            uurls={}
            tlds={}
            urlct={}
            while True:
                rows = self.cur.fetchmany(1000)
                if len(rows) > 0:
                    for row in rows:
                        ct+=1
                        if ct % 1000 == True:
                            print(ct/1000, 'mediadiet', platform)
                        if not 'http' in  row[0].lower():
                            continue
                        usr=row[1]
                        txt = row[0].split()
                        for u in txt:
                            if u.startswith('http'):
                                try:
                                    u = resolutions[u]
                                except KeyError:#Not resolved yet!
                                    print('unresolved',u)
                                    unresolved.append(u)
                                t=None
                                try:
                                    t = tldmap[u]
                                except KeyError:#Not tld'd yet!
                                    print('not tldd'),u
                                    untldd.append(u)
                                    # t = "%s.%s" % (t.domain, t.tld)
                                    # print(t)
                                if not t is None:
                                    try:
                                        tlds[usr].append(t)
                                    except KeyError:
                                        tlds[usr]=[t]



                                try:
                                    uurls[usr].append(u)
                                except KeyError:
                                    uurls[usr]=[u]
                                try:
                                    urlct[u] +=1
                                except KeyError:
                                    urlct[u]=1
                                if usr.isnumeric():
                                    numericusers.append(usr)#,row[2]))
                else:
                    break
            G=nx.DiGraph()
            for k,v in uurls.items():
                if not k is None:
                    for vv in v:
                        if not vv is None:
                            if G.has_edge(k, vv):
                                G[k][vv]['weight']+=1
                            else:
                                G.add_edge(k,vv, weight=1)
                            G.nodes[vv]['type']='url'
                            G.nodes[k]['type']='user'
            del uurls
            nx.write_gexf(G, '%s/mediadiet_%s.gexf'%(self.outdir,platform))
            G=nx.DiGraph()
            for k,v in tlds.items():
                if not k is None:
                    for vv in v:
                        if not vv is None:
                            if G.has_edge(k, vv):
                                G[k][vv]['weight']+=1
                            else:
                                G.add_edge(k,vv, weight=1)

                            G.nodes[vv]['type']='tld'
                            G.nodes[k]['type']='user'

            nx.write_gexf(G, '%s/mediadiet_%s_tlds.gexf'%(self.outdir,platform))
            out=[]
            for k,v in urlct.items():
                out.append([k,v])
            with codecs.open('%s/mediadiet_urlcounts_%s_ru.csv'%(self.outdir,platform), 'w', encoding='utf-8') as f:
                w=csv.writer(f, delimiter=';')
                w.writerows(out)
        with codecs.open('%s/untldd.csv'%self.outdir, 'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows([[rar] for rar in set(untldd)])
        with codecs.open('%s/unresolved.csv'%self.outdir, 'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows([[rar] for rar in set(unresolved)])
        for u in set(numericusers):
            print(u)


    def which_channels_do_debunkers_mention(self):
        #debunk cannels and which other te chans they mention
        import networkx as nx
        print('which_channels_do_debunkers_mention')
        channels=[]
        for m in os.listdir('debunkchans'):
            with codecs.open('debunkchans/%s' % m, 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=',')
                for row in r:
                    # print(row.keys())
                    try:
                        if not row['author_username'] in channels:
                            channels.append(row['author_username'])
                    except KeyError:
                        if not row['name'] in channels:
                            channels.append(row['name'])
        self.connect()
        ct=0
        mentionerg={}
        mentioners={}
        mentions={}
        G=nx.DiGraph()
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            print("Quey")
            cursor.execute("select urls, username, platform, language from posts where urls is not null and language='ru' and platform ='te'")
            for row in cursor:
                ct+=1
                if ct % 50000 == True:
                    print(ct, 'which_channels_do_debunkers_mention', len(mentioners))
                if not row[1] in channels:
                    continue
                isok=False
                for u in row[0].split(','):
                    
                    if u.lower().startswith('https://t.me/'):
                        key=row[1]
                        u=u.replace('/s/','')
                        if G.has_edge(key, u):
                            G[key][u]['weight']+=1
                        else:
                            G.add_edge(key, u, weight=1)
                        G.nodes[key]['type'] = 'debunker'
                        G.nodes[u]['type'] = 'channel'
             
        
        nx.write_gexf(G,'%s/mentions_by_debunk_channels.gexf'%self.outdir)
    def bigrams_after_phrase(self, phrase=None, language='ru'):
        #tail phrases for words after X 
        from nltk import sent_tokenize
        self.connect()
        afters={}
        ct=0
        phrase=phrase.lower()
        print('bigrams_after')
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 1000
            cursor.execute("select text from posts where language = %(l)s",{'l': language})
            for row in cursor:
                if ct % 1000 == True:
                    print(ct, 'bigrams_after', len(afters))
                txt=row[0].lower()
                if phrase in txt:
                    for sent in sent_tokenize(txt):
                        if phrase in sent:
                            tail = sent.split(phrase,1)[1]
                            # week = self.getWeekDate(row[1])
                            try:
                                afters[tail]+=1
                            except KeyError:
                                afters[tail] = 1

        out=[]
        # for week,v in afters.items():
            # l1=[week]
            # l2=["%s_val"%week]
            # for word2, val in sorted(v.items(), key=itemgetter(1), reverse=True):
                # l1.append(word2)
                # l2.append(val)
            # out.append(l1)
            # out.append(l2)
        fn='%s/rankflow_bigrams_after_%s_%s.csv' % (self.outdir, phrase,language)
        with codecs.open(fn,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows([[k,v] for k,v in sorted(afters.items(), key=itemgetter(1), reverse=True)])

    def bigrams_after_phrase_te_ruaff(self, phrase=None, platform ='te', language='ru'):
        #tail phrases for words after X 
        from nltk import sent_tokenize
        self.read_keywords()
        wants = self.kw['Allegations of disinformation']['Russian']
        wants += self.kw['Pro-war']['Russian']
        telemap={}
        with codecs.open('/var/scripts/parsers/telegram_info.csv','r',encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                telemap[row['name'].lower()] = (row['geo affiliation'], row['Position'], row['Position 2'])
        self.connect()
        afters={}
        ct=0
        phrase=phrase.lower()
        print('bigrams_after ruaffil',language,platform, phrase)
        okrows=0
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 1000
            if platform is None:
                query ="select username, text, keywords from posts where lower(text) like %(t)s and keywords is not null"
            else:
                query = "select username, text, keywords from posts where platform = '"+platform+"' and keywords is not null and lower(text) like %(t)s"
            if not language is None:
                query = query.replace('where',"where language = '%s' and"%language)
            #query += ' limit 100'
            cursor.execute(query,{'t': '%'+phrase.lower() +'%'})
            for row in cursor:
                ct+=1
                if ct % 1000 == True:
                    print(ct, 'bigrams_after_ruaffil', okrows, len(afters))
                topics = self.getTopicsFromRealKeywords(row[2])
                ok=False
                for t in topics:
                    if 'Russian' in " ".join(t):
                        if 'Allegations of dis' in " ".join(t) or 'Pr-war' in " ".join(t):
                            ok=True
                if not ok:
                    continue

                if platform =='te':#is None:
                    try:
                        if not telemap[row[0].lower()][0].lower() == 'russian':
                            continue
                    except KeyError:
                        continue
                okrows+=1
                txt=row[1].lower()
                if phrase.lower() in txt:
                    for sent in sent_tokenize(txt):
                        if phrase.lower() in sent.lower():
                            tail = sent.lower().split(phrase.lower(),1)[1]
                            # week = self.getWeekDate(row[1])
                            try:
                                afters[tail]+=1
                            except KeyError:
                                afters[tail] = 1
            print(len(afters), 'results', okrows, ct)

        out=[]
        fn='%s/bigrams_after_%s_%s_te_RU_aff_%s.csv' % (self.outdir, phrase,language,platform)
        with codecs.open(fn,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows([[k,v] for k,v in sorted(afters.items(), key=itemgetter(1), reverse=True)])



    def who_mentions_debunkers_graph(self):
        import networkx as nx
        print('who_mentions_debunkers')
        channels=[]
        for m in os.listdir('debunkchans'):
            with codecs.open('debunkchans/%s' % m, 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=',')
                for row in r:
                    try:
                        if not row['author_username'] in channels:
                            channels.append(row['author_username'])
                    except KeyError:
                        if not row['name'] in channels:
                            channels.append(row['name'])
        self.connect()
        ct=0
        mentionerg={}
        mentioners={}
        mentions={}
        G=nx.DiGraph()
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            print("Quey")
            cursor.execute("select urls, username, platform, language from posts where language='ru' and urls is not null")
            for row in cursor:
                ct+=1
                if ct % 50000 == True:
                    print(ct, 'who_mentions_debunkers', len(mentioners))
                isok=False
                for c in channels:
                    if 'https://t.me/%s' % c.lower() in row[0].lower() or 'https://t.me/s/%s' % c.lower() in row[0].lower():
                        key="%s_%s" %(row[2],row[1])
                        if G.has_edge(key, c):
                            G[key][c]['weight']+=1
                        else:
                            G.add_edge(key, c, weight=1)
                        G.nodes[key]['type'] = 'channel'
                        G.nodes[key]['platform'] = row[2]
                        G.nodes[c]['type'] = 'debunk'
             
        
        nx.write_gexf(G,'%s/mentions_of_debunk_channels.gexf'%self.outdir)
   
    def posts_mentioning_link(self, platforms = ['te','vk'], link='https://telegra.ph/Global-lies-over-Bucha-How-peoples-minds-are-manipulated-04-04'):
        self.connect()
        link = 'https://t.me/warfakes/1940'
        self.cur.execute("select url from urls where lower(resolve) like %(u)s", {'u': link.lower()})
        row=self.cur.fetchone()
        us=[link.lower()]

        while row:
            print(row)
            us.append(row[0].lower())
            row=self.cur.fetchone()
        out=[['link','date','platform','text','author','engagement','language','keywords']]
        for u in set(us):
            with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
                cursor.itersize = 100
                ct=0
                #print("select date, platform, text, username, engagement from posts where lower(text) like '%s'" % ("'%"+chan.lower().strip()+"%'"))
                cursor.execute("select date, platform, text, username, engagement, language, keywords from posts where lower(urls) like %(chan)s and platform in ('te','vk')", {'chan': "%"+ link.lower().strip()+"%", 'p': '"' + "','".join(platforms)+"'"})
                for row in cursor:
                    ct+=1
                    if ct % 10 == True:
                        print(ct, 'linkmentioners', len(out))
                    out.append([u]+list(row))
                print(len(out)-1,'results',u)
        with open('%s/link_mentioners.csv' % (self.outdir,), 'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)




    def who_mentions_debunkers(self):
        import networkx as nx
        print('who_mentions_debunkers!')
        channels=[]
        for m in os.listdir('debunkchans'):
            with codecs.open('debunkchans/%s' % m, 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=',')
                for row in r:
                    try:
                        if not row['author_username'] in channels:
                            channels.append(row['author_username'])
                    except KeyError:
                        if not row['name'] in channels:
                            channels.append(row['name'])
        self.connect()
        ct=0
        mentionerg={}
        mentioners={}
        mentions={}
        # G=nx.DiGraph()
        for x in os.listdir(self.outdir):
            if x.startswith('mentions_of') and x.endswith('.csv'):
                os.unlink('%s/%s' % (self.outdir, x))
        out=[['debunker','date','platform','text','author','engagement','language','keywords']]
        for chan in set(channels):
            with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
                cursor.itersize = 100
                print(chan)
                ct=0
                #print("select date, platform, text, username, engagement from posts where lower(text) like '%s'" % ("'%"+chan.lower().strip()+"%'"))
                cursor.execute("select date, platform, text, username, engagement, language, keywords from posts where lower(text) like %(chan)s", {'chan': "%"+chan.lower().strip()+"%"})
                for row in cursor:
                    ct+=1
                    if ct % 10 == True:
                        print(ct, 'who_mentions_debunkers', len(out))
                    out.append([chan]+list(row))
            print(len(out)-1,'results')
        with open('%s/mentions_of_debunkers.csv' % (self.outdir,), 'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)


    def makeNetwork(self, between=['username','keywords'],limit=False):#username should be first in list
        self.connect()
        if 'urls' in between or 'tlds' in between:
            self.connect2()
        if 'username' in between:
            q="select  " + ",".join(between) + ",platform from posts where "+between[0]+" is not null and "+between[1]+" is not null"
        if between[1] == 'topics':
            q=q.replace('topics','keywords')
        if between[1] == 'tlds':
            q=q.replace('tlds','urls')
        q=self.apply_filters(q)
        if limit:
            q += '  limit 20'
            
        urlmap={}
        # if between[1] in ['urls','tlds']:
            # resolutions, tldmap = self.getResolutions()
        print("Xec",q)
        self.cur.execute(q)
        mapke={}
        mem={}
        ct=0
        while True:
            rows = self.cur.fetchmany(10000)
            if len(rows) > 0:
                for row in rows:
                    ct+=1
                    if ct % 10000 == True:
                        print(ct)
                    key = row[0]
                    if 'username' in between:
                        key = "%s_%s" % (row[0], row[2])
                    was = [row[1]]
                    if between[1] in ['tlds','keywords','urls','hashtags','topics']:
                        was = row[1].split(',')
                        if between[1] == 'topics':
                            was = self.getTopicsFromKeywords(was)
                        elif between[1] == 'urls':
                            o=[]
                            for w in was:
                                try:
                                    o.append(mem[w])
                                except KeyError:
                                    self.cur2.execute("select resolve from urls where url = %(u)s", {'u':w})
                                    res = self.cur2.fetchone()
                                    
                                    if not res is None:
                                        o.append(res[0])
                                        mem[w] = res[0]
                                    else:
                                        o.append(w)
                                        mem[w]=w
                            was=o
                        elif between[1] == 'tlds':
                            mem, was = self.getTld(was, mem)
                            # o=[]
                            # for w in was:
                                # try:
                                    # o.append(mem[w])
                                # except KeyError:
                                    # self.cur2.execute("select tld from urls where url = %(u)s", {'u':w})
                                    # res = self.cur2.fetchone()
                                    # print(res)
                                    # if res is not None:
                                        # o.append(res[0])
                                        # mem[w] = res[0]
                                    # else:
                                        # w=w.split('/')[0].replace('https://','').replace('http://','')
                                        # o.append(w)
                                        # mem[w]=w
                            # was=o
                    for wa in was:
                        try:
                            mapke[key].append(wa)
                        except KeyError:
                            mapke[key] = [wa]
            else:
                break
        self.makeGraph(mapke, between)

    def sentences_that_mention(self, phrase, language=None, only_te_aff_rus=True):
        from nltk import sent_tokenize
        telemap={}
        with codecs.open('/var/scripts/parsers/telegram_info.csv','r',encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                telemap[row['name']] = (row['geo affiliation'], row['Position'], row['Position 2'])
        self.connect()
        afters={}
        ct=0
        phrase=phrase.lower()
        print('sents_mentioning',phrase)
        out=[['sentence']]
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 1000
            if only_te_aff_rus:
                cursor.execute("select text, username from posts where platform = 'te' and language = %(l)s and lower(text) like %(t)s",{'l': language, 't': '%'+ phrase + '%'})
            else:
                cursor.execute("select text, username from posts where language = %(l)s and lower(text) like %(t)s",{'l': language, 't': '%'+ phrase + '%'})
            for row in cursor:
                if ct % 1000 == True:
                    print(ct, 'sents_mention', phrase, len(out))
                if only_te_aff_rus:

                    try:
                        if not telemap[row[1]][0].lower() == 'russia':
                            continue
                    except KeyError:
                        continue
                txt=row[0].lower()
                if phrase in txt:
                    for sent in sent_tokenize(txt):
                        if phrase in sent:
                            out.append([sent])
        with codecs.open('%s/sentences_that_mention_%s_%s.csv' % (self.outdir, phrase, only_te_aff_rus),'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)

    def bigrams_after_phrase_for_debunk_channels(self, phrase=None, language='ru'):
        #tail phrases for words after X
        from nltk import sent_tokenize
        channels=[]
        for m in os.listdir('debunkchans'):
            with codecs.open('debunkchans/%s' % m, 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=',')
                for row in r:
                    # print(row.keys())
                    try:
                        if not row['author_username'] in channels:
                            channels.append(row['author_username'])
                    except KeyError:
                        if not row['name'] in channels:
                            channels.append(row['name'])
        self.connect()
        afters={}
        ct=0
        phrase=phrase.lower()
        print('bigrams_after for debunk')
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 1000
            cursor.execute("select text, username from posts where platform = 'te' and language = %(l)s",{'l': language})
            for row in cursor:
                if ct % 1000 == True:
                    print(ct, 'bigrams_after_DB', len(afters))
                if not row[1] in channels:
                    continue
                txt=row[0].lower()
                if phrase in txt:
                    for sent in sent_tokenize(txt):
                        if phrase in sent:
                            tail = sent.split(phrase,1)[1]
                            # week = self.getWeekDate(row[1])
                            try:
                                afters[tail]+=1
                            except KeyError:
                                afters[tail] = 1

        out=[]
        # for week,v in afters.items():
            # l1=[week]
            # l2=["%s_val"%week]
            # for word2, val in sorted(v.items(), key=itemgetter(1), reverse=True):
                # l1.append(word2)
                # l2.append(val)
            # out.append(l1)
            # out.append(l2)
        fn='%s/rankflow_bigrams_after_%s_%s_for_debunk_channels.csv' % (self.outdir, phrase,language)
        with codecs.open(fn,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows([[k,v] for k,v in sorted(afters.items(), key=itemgetter(1), reverse=True)])
            # w.writerows(zip_longest(*out))

    def getTld(self, was, mem):
        self.connect2()
        o=[]
        for w in was:
            try:
                o.append(mem[w])
            except KeyError:
                self.cur2.execute("select tld from urls where url = %(u)s", {'u':w})
                res = self.cur2.fetchone()
                if res is not None:
                    o.append(res[0])
                    mem[w] = res[0]
                else:
                    w=w.split('/')[0].replace('https://','').replace('http://','')
                    o.append(w)
                    mem[w]=w
        return [mem, o]
    
    def getTldCount(self):
        self.connect()
        self.cur.execute("select url, tld from urls where tld is not null")
        row=self.cur.fetchone()
        a={}
        while row:
            a[row[0]] = row[1]
            row=self.cur.fetchone()
        tldc={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            cursor.execute("select urls, keywords from posts where urls is not null and keywords is not null and language = 'ru'")
            for row in cursor:
                for u in row[0].split(','):
                    try:
                        u=a[u]
                    except KeyError:
                        pass
                    topics = self.getTopicsFromKeywords(row[1].split(','))
                    for t in topics:
                        try:
                            tldc[t]
                        except KeyError:
                            tldc[t]={}
                        try:
                            tldc[t][u] +=1
                        except KeyError:
                            tldc[t][u] =1
        out=[]
        for k,v in tldc.items():
            for kk, vv in v.items():
                out.append([k,kk,vv])
        with codecs.open('%s/tldc_%s.csv' % (self.outdir,"RU"), 'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)
        

    def makeGraph(self, dic, between):
        G=nx.DiGraph()
        for k,v in dic.items():
            if not k is None:
                for vv in v:
                    if not vv is None:
                        if G.has_edge(k, vv):
                            G[k][vv]['weight']+=1
                        else:
                            G.add_edge(k,vv, weight=1)

                        G.nodes[vv]['type']=between[0]
                        G.nodes[k]['type']=between[1]
                            
        plus=''
        for k,v in self.filters.items():
            plus += '_%s' % v
        nx.write_gexf(G, '%s/nw_%s_%s_%s.gexf'%(self.outdir, between[0], between[1], plus))

    def getTopicsFromRealKeywords(self, kws):
        if self.kw == {}:
            self.read_keywords()
        if kws is None:
            return []
        topics=[]
        kws = kws.split(',')
        for k,v in self.kw.items():
            for kk,words in v.items():
                for w in words:
                    if w in kws:
                        topics.append((k,kk))
        return topics
    def getTopicsFromKeywords(self, kws):
        if self.kw == {}:
            self.read_keywords()
        if kws is None:
            return []
        topics=[]
        for k,v in self.kw.items():
            for kk,words in v.items():
                for w in words:
                    for q in kws:
                        isin=False
                        if w.endswith('*'):
                            if q.replace('*','').lower().startswith(w.lower().replace('*','')):
                                isin=True
                        else:
                            if w.lower() == q.lower():
                                isin=True
                        if isin:
                            stance = '%s: %s'% (k,kk)
                            topics.append(stance)
        return topics
    

    def procrow(self, row, per, dic, fields_to_sum, fields_to_count):
        lst=[row]
        if 'topic' in per or 'keywords' in per:
            lst=[]
            kws={}
            stances={}
            
            if 'topic' in per:
                stances = self.getTopicsFromKeywords(row['keywords'])
            if 'keywords' in per and not  row['keywords'] is None:
                kws = row['keywords'].split(',')
            if 'topic' in per:
                for stance in stances:#we're not counting occurrences here
                    row2=dict(row)
                    row2['topic'] = stance
                    if not 'keywords' in per:#if both, it gets added in the next one
                        lst.append(row2)
            if 'keywords' in per:
                for stance in kws:#we're not counting occurrences here
                    if not 'topic' in per:
                        row2=dict(row)
                    row2['keywords'] = stance
                    lst.append(row2)
        mem={}
        for ei, row in enumerate(lst):
            key = []
            for p in per:
                if p == 'week':
                    dt = row['date']#datetime.strptime(row['date'], '%Y-%m-%d')
                    dt = "%s-W%s" % (dt.year, dt.isocalendar()[1])
                    dt = str(datetime.strptime(dt + '-1', "%Y-W%W-%w"))[:10]
                    key.append(str(dt))
                else:
                    if p =='tlds':
                        p='urls'
                        was=[]
                        if not row['urls'] is None:
                            mem, was = self.getTld(row['urls'].split(','), mem)
                        for w in was:
                            key.append(w)
            key = [qui for qui in key if qui]
            key = self.seppy.join(key)
            try:
                dic[key]
            except KeyError:
                dic[key]={}
            for f in fields_to_sum:
                try:
                    dic[key][f] += row[f]
                except KeyError:
                    dic[key][f] = row[f]
            for f in fields_to_count:
                try:
                    dic[key][f] += 1
                except KeyError:
                    dic[key][f] = 1
                if f == 'engagement' and dic[key][f] is None:
                    dic[key][f] = 0
        return dic


    def modfieldname(self, h, fields_to_sum, fields_to_count):
        hh=h
        if h in fields_to_sum:
            hh = "%s_sum"%h
        elif h in fields_to_count:
            hh = "%s_count"%h
        if hh == 'lid':
            hh='post'
        if hh == 'lid_count':
            hh='post_count'
        return hh


    def addExtraFields(self, extrafields, out):
        telemap={}
        if 'telegramdata' in extrafields:
            extrafields.remove('telegramdata')
            with codecs.open('/var/scripts/parsers/telegram_info.csv','r',encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=';')
                for row in r:
                    telemap[row['name']] = (row['geo affiliation'], row['Position'], row['Position 2'])
        if not extrafields == []:
            flds = ",".join(extrafields)
            flds = flds.replace(',telegramdata','')
            newout=[]
            for o in out:
                qqqq="select %s from posts where lid = '%s'" % (flds, o['post'])
                self.cur.execute(qqqq)
                row = self.cur.fetchone()
                for ieq, f in enumerate(extrafields):
                    o[f] = row[ieq]
                try:
                    td = telemap[o['username']]
                    o['geo_affil'] = td[0]
                    o['Position'] = td[1]
                    o['Position 2'] = td[2]
                except KeyError:
                    pass
                newout.append(o)
            return newout
        else:
            return out



    def is_summable(self, fields_to_sum, ddic):
        ok=True
        if not fields_to_sum == []:
            for f in fields_to_sum:
                if ddic[f] is None:
                    ok=False
                    break
        return ok
    
    
    def problematic_urls(self):
        print("Problematicurls")
        self.connect()
        self.cur.execute("select * from urls where resolve is null or resolve = 'ERROR' or tld is null")
        out=[['url','resolved_url','tld']]
        while True:
            rows = self.cur.fetchmany(1000)
            if len(rows) > 0:
                for row in rows:
                    out.append(row)
            else:
                break
        with codecs.open('%s/problematic_urls.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f,delimiter=';')
            w.writerows(out)
    
    def telinks(self):
        # do you have a list of all the Telegram channels that have been mentioned in Russian posts (in the form of e.g. a telegram url) and their corresponding counts?
        self.connect()
        self.cur.execute("select url, resolve from urls where tld = 't.me' or tld = 'telegram.org'")
        tm={}
        rm={}
        while True:
            rows = self.cur.fetchmany(1000)
            if len(rows) > 0:
                for row in rows:
                    rm[row[1]] = row[1]
                    rm[row[0]] = row[1]
            else:
                break
        out=[['url','count']]
        m={}
        self.cur.execute("select urls from posts where language = 'ru' and urls like '%t.me%'")
        while True:
            rows = self.cur.fetchmany(1000)
            if len(rows) > 0:
                for row in rows:
                    urls = row[0].split(',')
                    for u in urls:
                        try:
                            resolve = rm[u]
                            try:
                                m[resolve]+=1
                            except KeyError:
                                m[resolve]=1
                        except KeyError:
                            pass
            else:
                break
        for k,v in sorted(m.items(), key=itemgetter(1), reverse=True):
            out.append([k,v])
        with codecs.open('%s/telegram_counts_russian.csv'%self.outdir,'w', encoding='utf-8') as f:
                w=csv.writer(f,delimiter=';')
                w.writerows(out)

#engagement	url	topic	platform

    def getTopUrls(self,tlds_instead=True, counts_instead=False):
        self.connect()
        tldmap={}
        if tlds_instead:
            self.cur.execute("select url, resolve, tld from urls")
            while True:
                rows = self.cur.fetchmany(10000)
                if len(rows) > 0:
                    for row in rows:
                        tldmap[row[1]] = row[2]
                        tldmap[row[0]] = row[2]
                else:
                    break
            
        for pf in ['vk','te','tw','fb']:
        # for pf in ['tw','fb']:
            self.cur.execute("select engagement, urls, keywords from posts where engagement is not null and keywords is not null and urls is not null and platform = %(p)s and language = 'ru'", {'p':pf})
            out=[['url','topic','engagement']]
            if counts_instead:
                out[0][2] = 'count'
            if tlds_instead:
                out[0][0] = 'domain'
            m={}
            cta=0
            while True:
                rows = self.cur.fetchmany(10000)
                if len(rows) > 0:
                    for row in rows:
                        cta+=1
                        if cta % 1000 == True:
                            print('--', cta/1000, pf, tlds_instead)
                        topics = self.getTopicsFromKeywords(row[2].split(','))
                        for t in set(topics):
                            for url in row[1].split(','):
                                if tlds_instead:
                                    try:
                                        url = tldmap[url]
                                    except KeyError:
                                        pass
                                try:
                                    m[url]
                                except KeyError:
                                    m[url]={}
                                val = row[0]
                                if counts_instead:
                                    val=1
                                try:
                                    m[url][t]+=val
                                except KeyError:
                                    m[url][t]=val
                else:
                    break
            for k,v in m.items():
                for kk,vv in sorted(v.items(), key=itemgetter(1), reverse=True):
                    out.append([k,kk,vv])
            del m
            name = '%s/engagement_urls_%s.csv' % (self.outdir,pf)
            if tlds_instead:
                name = name.replace('urls','tlds')
            if counts_instead:
                name = name.replace('engagement','counts')
            with codecs.open(name,'w', encoding='utf-8') as f:
                w=csv.writer(f,delimiter=';')
                w.writerows(out)

    def getDicFromDb(self,query, fields_to_sum, fields_to_count, per):
        print(query)
        query = query.replace('FROM posts and','FROM posts where')
        self.cur.execute(query)
        desc = [a[0] for a in self.cur.description]
        dic = {}
        cta=0
        while True:
            rows = self.cur.fetchmany(10000)
            if len(rows) > 0:
                for row in rows:
                    cta+=1
                    if cta % 1000 == True:
                        print('--', cta/1000)
                    ddic=dict(zip(desc,row))
                    
                    if self.is_summable(fields_to_sum, ddic):
                        dic = self.procrow(ddic, per, dic, fields_to_sum, fields_to_count)
            else:
                break
        return dic


    def splitDict(self, lst, splitper):
        if splitper is None:
            dd = {'':lst}
        else:
            dd={}
            for l in lst:
                a=l[0].split(self.seppy)
                az=dict(zip(per, a))
                if type(splitper) == list:
                    val = ''
                    for squ in splitper:
                        val += az[squ]
                else:
                    val = az[splitper]
                try:
                    dd[val].append(l)
                except KeyError:
                    dd[val] = [l]
        return dd
    
    
    def splitTopic(self, di):
        if 'topic' in di.keys() and ':' in di['topic']:
            t1, t2 = di['topic'].split(':')
            di['topic'] = t1
            di['keyword language'] = t2
        return di


    def prowar_disinfo_ru_stati(self):
        self.read_keywords()
        wants = self.kw['Allegations of disinformation']['Russian']
        wants += self.kw['Pro-war']['Russian']
        self.connect()
        ids2test=[]
        for i, w in enumerate(wants):
            print(i, len(wants), w, len(set(ids2test)))
            w=w.lower().replace('*','')
            ww='%'+w+'%'
            self.cur.execute("select platformid, keywords from posts where platform = 'tw' and keywords like %(w)s ", {'w': ww})
            row = self.cur.fetchone()
            while row:
                # print(row)
                kw = row[1].lower().split(',')
                if w in kw:
                    # print('yay')
                    ids2test.append(row[0])
                row = self.cur.fetchone()
            ids2test=list(set(ids2test))
        print(len(set(ids2test)))
        pickle.dump(set(ids2test), open('totesttwurls.p','wb'))


    def gettopic(self, keyword):
        if self.kw == {}:
            self.read_keywords()
        for k, v in self.kw.items():
            for kk, vv in v.items():
                wap = [q.lower().strip().replace('*','') for q in vv]
                if keyword.lower().strip().replace('*','') in wap:
                    return " - ".join([k,kk])

    def rankflow_db(self, languages=None, platforms=None):
        print('rankflowsddb!')
        from nltk import skipgrams
        # Rankflow per week of bigrams, word embeddings or parts of sentence (distant reading) for:
        # War (війни in UA, война in RU)
        #wwords = ['war', 'війни', 'война',
        # Putin (Пу́тин in RU, Путін in UA)
        #'putin', 'Пу́тин', 'Путін',
        # Kremlin (Кремль in RU, Кремль in UA)
        #'kremlin','Кремль', 'Кремль',
        # UN (OOH in both languages)
        #'un','OOH',
        #'NATO','НАТО','НАТО',
        # EU (ЄС in UA, EC in RU)        
        #'EU','ЄС','EC']
        #wwords = [w.lower().strip() for w in wwords]   
        tokenizer = TweetTokenizer()
        # for platform in ['te','fb','vk','re','tw']:
        for x in os.listdir(self.outdir+'/rankflows'):
            os.unlink('%s/rankflows/%s' % (self.outdir, x))
        self.connect()
        if languages is None:
            languages = ['uk', 'ru']
        if platforms is None:
            platforms = ['te','fb','vk','tw']
        for language in languages:#['uk','ru']:
            dic={}
            print(language)
            wrds = ['война', 'Пу́тин','Кремль','OOH','НАТО','EC','Фейк','Украинский']
            if language == 'uk':
                wrds = ['війни', 'Путін', 'Кремль', 'OOH', 'НАТО','EC','Фейк','Російський']
            stops = codecs.open('%s_stops.txt' % language,'r', encoding='utf-8').read().split("\n")
            stops = [a.strip() for a in stops]
            #for w in wrds:
            if True:
                for platform in platforms:#['te','fb','vk','tw']:
                    ct=0
                    av=0
                    self.cur.execute("select count(*) from posts where platform = '%s' and language = '%s'" % (platform, language))
                    row = self.cur.fetchone()
                    tot = row[0]
                    with self.conn.cursor(name='name_of_cursora%s'%randint(0,999999)) as cursor:

                        cursor.itersize = 4000
                        print(platform, language)
                        cursor.execute("select date, text from posts where platform = '%s' and  language = '%s'" % (platform, language))
                        #%(l)s and text like %(t)s", {
                        #    'l': language,
                        #    't': "%"+w.strip()+"%"
                        #})
                        av=0
                        for xx in cursor:

                        #adic = self.compile_query(qquery=w, only_lans=[language],per=['lid','language','date','text'], fields_to_sum=[],fields_to_count=[], doreturn=True)#, only_platform=platform)
                        #for ct, x in enumerate(adic):
                            ct+=1
                            if ct % 10000 == True:
                                print("%s/%s"% (ct/1000, tot/1000), platform, language)
                            isok=False
                            for w in wrds:
                                if w.lower() in xx[1].lower():
                                    isok=True
                            if isok:
                                # if ct > 500:
                                    # break
                                #lid = x['lid']
                                #lan = x['language']
                                #if not lan == language:
                                #    continue
                                dt=xx[0]#datetime.strptime(xx[0], "%Y-%m-%d")
                                
                                
                                dt = "%s %s" % (dt.year, dt.isocalendar()[1])

                                #txt = simple_preprocess(x['text'].replace('#',' ').lower(),max_len=66)
                                txt = [a for a in tokenizer.tokenize(xx[1]) if not a in stops]

                                
                                res = list(skipgrams(txt, 2, 3))
                                words=[]
                                for w in wrds:
                                    for x in res:
                                        if x[0] == x[1]:
                                            continue
                                        leword=None
                                        if x[0] == w and not x[1] == w:
                                            leword = x[1]
                                        elif x[1] == w and not x[0] == w:
                                            leword =x[0]
                                        if not leword is None:
                                            try:
                                                dic[w]
                                            except KeyError:
                                                dic[w]={}
                                            try:
                                                dic[w][dt]
                                            except KeyError:
                                                dic[w][dt]={}
                                            try:
                                                dic[w][dt][leword] += 1
                                            except KeyError:
                                                dic[w][dt][leword] = 1
                                            av+=1
                    print(ct, av,'<<')    
                        
                print(w, ct,'rows',av)
                for word,qv in sorted(dic.items()):
                    out=[]
                    for k,v in sorted(qv.items()):
                        lo1=[k]
                        lo2=["%s_val"%k]
                        for kk,vv in v.items():
                            lo1.append(kk)
                            lo2.append(vv)
                        out.append(lo1)
                        out.append(lo2)
                    with codecs.open('%s/rankflows/%s_%s.csv' % (self.outdir, language,word),'w', encoding='utf-8') as f:
                        w=csv.writer(f, delimiter=';')
                        w.writerows(zip_longest(*out))
    def testDomainErrors(self):
        self.connect()
        # pickle.dump({},open('tlderrresult.p','wb'))
        from selenium import webdriver
        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        options.add_argument('ignore-certificate-errors')
        options.add_argument('log-level=3')
        browser = webdriver.Chrome(options=options)
        browser.set_page_load_timeout(60)
        
        self.cur.execute("select count(*) from urls where resolve = 'ERROR' ")
        res=self.cur.fetchone()
        tot=res[0]
        self.cur.execute("select url from urls where resolve = 'ERROR' order by random()")
        row=self.cur.fetchone()
        rez=pickle.load(open('tlderrresult.p','rb'))
        ct=0
        while row:
            ct+=1
            print(ct,tot)
            if ct % 20 == True:
                ld=False
                while not ld:
                    try:
                        rez2=pickle.load(open('tlderrresult.p','rb'))
                        ld=True
                    except Exception:
                        time.sleep(1)
                for k,v in rez2.items():
                    rez[k]=v
                pickle.dump(rez, open('tlderrresult.p','wb'))
            if row[0].count('/') < 2:
                pass
            else:
                if row[0].count('/') > 2:
                    a = '/'.join(row[0].split('/')[:3])
                else:
                    a=row[0]
                print(a)
                try:
                    rez[a]
                except KeyError:
                    try:
                        browser.get(a)
                        time.sleep(1)
                        # txt = browser.find_element_by_class_name("html").text
                        txt = browser.find_element_by_xpath("/html/body").text
                        rez[a]=txt
                        # print(txt)
                    except Exception as er:
                        # print(er)
                        rez[a] = str(er)
            row=self.cur.fetchone()
        pickle.dump(rez, open('tlderrresult.p','wb'))
    def readDomainErrors(self):
        a=pickle.load(open('tlderrresult.p','rb'))
        b={}
        for k, v in a.items():
            q = k.replace('www.','')
            q='.'.join(q.split('.')[1:])
            q=k.replace('www.','').replace('https://','').replace('http://','')
            # if q.endswith('cloudfront.net'):
                # q='cloudfront.net'
            q=q.rsplit('.')[-1]
            try:
                b[q]
            except KeyError:
                b[q]={}
            v=v.replace('  (Session info: headless chrome=99.0.4844.84)','').strip()
            v=v.replace('Message: unknown error: net::','')
            v=v.replace('nginx/1.18.0 (Ubuntu)','')
            if '403' in v:
                v = '403 Forbidden'
            try:
                b[q][v]+=1
            except KeyError:
                b[q][v]=1
        out=[]
        for k,v in b.items():
            for kk, vv in v.items():
                if vv > 1:
                    out.append([k,kk,vv])
        with codecs.open('%s/tldstatus.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f,delimiter=';')
            w.writerows(out)
    def readTweetStatus(self):
        res=pickle.load(open('totesttwurls_res.p','rb'))
        boo = ['Something went wrong. Try reloading.','This Tweet violated the Twitter Rules.','This Tweet is from a suspended account.','This Tweet is from an account that no longer exists.','this account owner limits who can view their Tweets','Hmm... this page doesn’t exist.','This content might not be appropriate for people under 18 years old.']
        moo={'OK':0}
        ks=list(res.keys())
        shuffle(ks)
        baa= ['Did someone say … cookies?','Twitter and its partners use cookies to provide you with a better, safer and faster service and to support our business. Some cookies are necessary to use our services, improve our services, and make sure they work properly. Show more about your choices.','Log in Sign up','Accept all cookies Refuse non-essential cookies','Don’t miss what’s happening People on Twitter are the first to know.']
        # for k,v in res.items():
        redo=[]
        for k in ks:
            v=res[k]
            if not type(v) == dict:
                print([k,v])
            if v['err'] == True:
                txt= " ".join(v['text'].split())
                for b in baa:
                    txt=txt.replace(b,'').strip()
                ff=False
                for b in boo:
                    if b in txt:
                        try:
                            moo[b]+=1
                        except KeyError:
                            moo[b]=1
                        ff=True
                        break
                if not ff:
                    if txt in ['Thread','Tweet'] or txt.startswith('Thread Conversation') or txt.startswith('Tweet Conversation'):
                        moo['OK'] += 1
                        ff=True
                if not ff:
                    print([k,txt,v])
                    redo.append(k)
            else:
                moo['OK']+=1
        out=[['count','text']]
        for k,v in moo.items():
            print([k[:50], v])
            out.append([v,k])
        with codecs.open('%s/tweetstatus.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f,delimiter=';')
            w.writerows(out)
        print("REDO")
        print(redo)
    def howmanyUrls(self):
        self.connect()
        self.cur.execute(" select count(*) from urls")
        row=self.cur.fetchone()
        print(row)
    def testTweetStatus(self):
        from selenium import webdriver
        options = webdriver.ChromeOptions();
        # options.add_argument('log-level=3')
        # pickle.dump({},open('totesttwurls_res.p','wb'))
        # sys.exit()
        options.add_argument('headless')
        browser = webdriver.Chrome(options=options)

        wants = pickle.load(open('totesttwurls.p','rb'))
        dta = list(wants)
        shuffle(dta)
        ld = False
        bct=0
        while not ld:
            if bct > 10:
                res=pickle.load(open('totesttwurls_res_bu.p','rb'))
                ld=True
            try:
                res=pickle.load(open('totesttwurls_res.p','rb'))
                ld=True
            except Exception:
                bct+=1
        
        # res={}
        # for k,v in res.items():
            # if not type(res) == dict:
                # res[k] = {'text': v,'err':False,'when':datetime.now()}
        changed=False
        # del res['1483233497150021636']
        # dta = ['1483233497150021636']+dta
        for i, d in enumerate(dta):
            print(i/1000, len(dta)/1000, len(res)/1000)
            if i % 10 == True and changed:
                ld=False
                while not ld:
                    try:
                        res2=pickle.load(open('totesttwurls_res.p','rb'))
                        ld=True
                    except Exception as er:
                        print(er)
                        time.sleep(1)
                for k,v in res2.items():
                    try:
                        res[k]
                    except KeyError:
                        # if type(v) == str:
                            # res[k]={'err':False,'text':v,'when':datetime.now()}
                        # else:
                        res[k]=v

                # for k,v in res.items():
                    # if not type(v) == dict:
                        # res[k] = {'text': v,'err':False,'when':datetime.now()}
                    # elif type(v['text']) == dict:
                        # res[k]['text'] = v['text']
                # for k,v in res.items():
                    # if not type(v) == dict:
                        # sys.exit('WTF')
                pickle.dump(res, open('totesttwurls_res.p','wb'))
                if randint(0,10) == 4:
                    pickle.dump(res, open('totesttwurls_res_bu.p','wb'))
                changed=False
            try:
                res[d]
                continue
            except KeyError:
                pass
            changed=True
            url = "https://twitter.com/twitter/status/%s" % d
            try:
                browser.get(url)
                time.sleep(1)

                tweets = browser.find_elements_by_css_selector("[data-testid=\"tweet\"]")
                if len(tweets) < 1:
                    body = browser.find_element_by_tag_name('body')
                    res[d] = {'err': True, 'text': body.text, 'when':datetime.now()}
                    print(body.text)
                else:
                    for tweet in tweets[:1]:
                        t=tweet.get_attribute('innerHTML')

                        res[d] = {'err': False, 'text': tweet.text, 'when':datetime.now()}
                        # print(tweet.text)
                        # print("OK")
            except Exception as erre:
                print(erre)
                print(url)
            time.sleep(1)
        pickle.dump(res, open('totesttwurls_res.p','wb'))
                
            


    def fixFieldNames(self, per, v, fields_to_sum, fields_to_count, k, hdz):
        a=k.split(self.seppy)
        #print('A',a)
        #print('per',per)
        az=dict(zip(per, a))
        #print('AZ',az)
        if hdz is None:
            hdz = per + list(v.keys())
            if 'tlds' in hdz:
                hdz.remove('tlds')
                hdz.append('urls')
        di = {}
        for h in hdz:
            # print(h,az)
            hh=h
            if h in fields_to_sum:
                hh = "%s_sum"%h
            elif h in fields_to_count:
                hh = "%s_count"%h
            if hh == 'lid':
                hh='post'
            if hh == 'lid_count':
                hh='post_count'
            if 'tlds' in per:
                hh='tlds'
                h='tlds'
            try:
                di[hh] = v[h]
            except KeyError:
                di[hh] = az[h]
        return di
    def getStanceFromText(self, txt,returnFoundWords=False):
        if self.kw == {}:
            self.read_keywords()
        case_sensitive = ['ООН','НАТО','EC','ЛНР','ДНР','SWIFT','ООН','НАТО','ЄС']
        case_sensitive=[]
        if txt is None:
            if returnFoundWords:
                return []
            else:
                return ''
        #txt = simple_preprocess(txt.replace('#',' ').lower(),max_len=66)
        txt = self.tokenizer.tokenize(txt.replace('#',' ').lower())
        stances = {}
        kws=[]
        for k,v in self.kw.items():
            for kk,words in v.items():
                for w in words:
                    w=w.replace('#','')#does this make up for the difference?
                    if not w in case_sensitive:
                        w=w.lower()
                        ltxt = [uau.lower() for uau in txt]
                    if 'stoprussianhate' in w.lower() or 'ussophobia' in w.lower():
                        w=w.lower()
                    stance = '%s: %s'% (k,kk)
                    if w.endswith('*'):
                        numbers=0
                        w=w[:-1]
                        for qq in ltxt:
                            if qq.startswith(w):
                                numbers+=1
                                kws.append(w)
                    else:
                        numbers = ltxt.count(w)
                        for i in range(0,numbers):
                            kws.append(w)
                    if numbers > 0:
                        try:
                            stances[stance] += numbers
                        except KeyError:
                            stances[stance] = numbers
        if returnFoundWords:
            return [kws, stances]
        return ','.join(stances)


    def build_query(self, only_platform, only_lans, qquery, after_date, before_date, username):
        if only_platform is not None:
            query = "SELECT * FROM posts where platform = '%s'" % only_platform
        else:
            query = "SELECT * FROM posts"
        if not only_lans == []:
            query += " and language in ('" + "','".join(only_lans) + "')"
        if not after_date is None:
            query += " and date >= '%s'" % after_date
        if not username is None:
            query += " and username = '%s'" % username
        if not qquery is None:
            query += ' and lower(text) like \'%'+ qquery.lower()+ '%\''
        if after_date is not None:
            query += " and date >= '" + after_date + "'"
        if before_date is not None:
            query += " and date <= '" + before_date + "'"
        if ' and ' in query and not ' where ' in query:
            query = query.split(' and ',1)
            query = " where ".join(query)
        return query

    def tld_online_check(self):
        import requests
        from requests.packages.urllib3.exceptions import InsecureRequestWarning

        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

        yesterday = date.today() - timedelta(days=1)
        self.connect()
        self.connect2()
        self.cur.execute("select distinct(tld) from urls where tld is not null")
        ct=0
        while True:
            rows = self.cur.fetchmany(10000)
            if len(rows) > 0:
                for row in rows:
                    ct+=1
                    if ct % 100 == True:
                        print('CheckTLD', ct)
                    err = 'Accessible'
                    #http or https?
                    # self.cur2.execute("select url from urls where tld = %(u)s and resolve like %(z)s", {'u': row[0], 'z': '%.%'})
                    # rab = self.cur2.fetchone()
                    self.cur2.execute("select checked_at from tld_accessible where tld = %(u)s order by checked_at desc limit 1", {'u': row[0]})
                    rew = self.cur2.fetchone()
                    if not rew or rew[0].date() < yesterday:
                        try:
                            r = requests.head('https://'+ row[0], allow_redirects=True, timeout=10, verify=False)
                            # updates[row[0]] = r.url
                        except Exception as err2:
                            err=str(err2)
                            print("Nope", row[0], err2)

                        self.cur2.execute("insert into tld_accessible (tld, status) values ( %(r)s, %(u)s)", {
                            'r': row[0][:399],
                            'u': err[:399]
                        })
                        self.conn2.commit()

            else:
                break

    def parseTopics(self, per, row):
        if 'topic' in per or 'keyword' in per:
            lst=[]
            kws, stances = self.getStanceFromText(row['text'], returnFoundWords=True)
            if 'topic' in per:
                for stance in stances:#we're not counting occurrences here
                    row2=dict(row)
                    row2['topic'] = stance
                    if not 'keyword' in per:#if both, it gets added in the next one
                        lst.append(row2)
        return lst

    def parseKWS(self, per, row, lst):
        if 'keyword' in per:
            for stance in kws:#we're not counting occurrences here
                if not 'topic' in per:
                    row2=dict(row)
                row2['keyword'] = stance
                lst.append(row2)
        return lst

    def makeKey(self, per, row):
        key = []
        for p in per:
            if p == 'week':
                dt = row['date']#datetime.strptime(row['date'], '%Y-%m-%d')
                dt = "%s-W%s" % (dt.year, dt.isocalendar()[1])
                dt = str(datetime.strptime(dt + '-1', "%Y-W%W-%w"))[:10]
                key.append(str(dt))
            else:
                key.append(str(row[p]))
        key = self.seppy.join(key)
        return key
    def sumAndCountFields(self, fields_to_sum, fields_to_count, dic, row):
        for f in fields_to_sum:
            try:
                dic[key][f] += row[f]
            except KeyError:
                dic[key][f] = row[f]
        for f in fields_to_count:
            try:
                dic[key][f] += 1
            except KeyError:
                dic[key][f] = 1
            if f == 'engagement' and dic[key][f] is None:
                dic[key][f] = 0
        return dic

    def process_row(self, row, per, dic, fields_to_sum, fields_to_count):
        lst=[row]
        if 'topic' in per or 'keyword' in per:
            lst=[]
            kws, stances = self.getStanceFromText(row['text'], returnFoundWords=True)
            # kws, stances = self.OldGetStanceFromText(row['text'], returnFoundWords=True)
            # stances = self.getStanceFromText(row['text'], returnFoundWords=True)
            if 'topic' in per:
                for stance in stances:#we're not counting occurrences here
                    row2=dict(row)
                    row2['topic'] = stance
                    if not 'keyword' in per:#if both, it gets added in the next one
                        lst.append(row2)
            if 'keyword' in per:
                for stance in kws:#we're not counting occurrences here
                    if not 'topic' in per:
                        row2=dict(row)
                    row2['keyword'] = stance
                    lst.append(row2)
        for ei, row in enumerate(lst):
            # if ei % 1000 == True:
                # print('proc-%s'% ei, len(lst))
            key = []
            for p in per:
                if p == 'week':
                    dt = row['date']#datetime.strptime(row['date'], '%Y-%m-%d')
                    dt = "%s-W%s" % (dt.year, dt.isocalendar()[1])
                    dt = str(datetime.strptime(dt + '-1', "%Y-W%W-%w"))[:10]
                    key.append(str(dt))
                else:
                    key.append(str(row[p]))
            key = self.seppy.join(key)
            try:
                dic[key]
            except KeyError:
                dic[key]={}
            for f in fields_to_sum:
                try:
                    dic[key][f] += row[f]
                except KeyError:
                    dic[key][f] = row[f]
            for f in fields_to_count:
                try:
                    dic[key][f] += 1
                except KeyError:
                    dic[key][f] = 1
                if f == 'engagement' and dic[key][f] is None:
                    dic[key][f] = 0
        return dic
        #lst=[dic]
        #lst = self.parseTopics(per, dic)
        #lst = self.parseKWS(per, dic)
        #for ei, row in enumerate(lst):
        #    key = self.makeKey(per, row)
        #    try:
        #        gatherdic[key]
        #    except KeyError:
        #        gatherdic[key]={}
        #    dic = self.sumAndCountFields(fields_to_sum, fields_to_count, dic, row)
        #return dic
    def splitPerList(self, per, splitper, lst):

        if splitper is None:
            dd = {'':lst}
        else:
            dd={}
            for l in lst:
                a=l[0].split(self.seppy)
                az=dict(zip(per, a))
                if type(splitper) == list:
                    val = ''
                    for squ in splitper:
                        val += az[squ]
                else:
                    val = az[splitper]
                try:
                    dd[val].append(l)
                except KeyError:
                    dd[val] = [l]
        return dd
    def makeList(self, dic, top):
        lst=[]
        if top > 0:
            for x in sorted(dic,key=lambda x:int(dic[x]['engagement']), reverse=True):
                lst.append([x, dic[x]])
        else:
            for k,v in dic.items():
                lst.append([k,v])
        return lst

    def makeHeads(self, per, v, fields_to_sum, fields_to_count):
        hdz = per + list(v.keys())
        hdznew=[]
        for h in hdz:
            if h in fields_to_sum:
                hdznew.append("%s_sum"%h)
            elif h in fields_to_count:
                hdznew.append("%s_count"%h)
            else:
                hdznew.append(h)
            if h == 'lid':
                h='post'
            if h == 'lid_count':
                h='post_count'
        return hdz

    def makeOut(self, dd, per, fields_to_sum, fields_to_count, top):
        outs=[]
        hdz=None
        for asfaefaw, lst in dd.items():
            out=[]
            ct=0
            for ei, x in enumerate(lst):
                if ei % 1000 == True:
                    print('p',ei, len(lst), per)
                k=x[0]
                v=x[1]
                ct+=1
                if top > 0 and ct > top-1:
                    break
                a=k.split(self.seppy)
                az=dict(zip(per, a))
                if hdz is None:
                    hdz=self.makeHeads( per, v,fields_to_sum, fields_to_count)
                di = {}
                for h in hdz:
                    hh=h
                    if h in fields_to_sum:
                        hh = "%s_sum"%h
                    elif h in fields_to_count:
                        hh = "%s_count"%h
                    if hh == 'lid':
                        hh='post'
                    if hh == 'lid_count':
                        hh='post_count'
                    try:
                        di[hh] = v[h]
                    except KeyError:
                        di[hh] = az[h]
                if 'topic' in di.keys() and ':' in di['topic']:
                    t1, t2 = di['topic'].split(':')
                    di['topic'] = t1
                    di['keyword language'] = t2
                if not di in out:
                    out.append(di)
            outs.append((asfaefaw,out))
        return outs

    def top100posts(self, start='2022-04-28', end='2022-05-11'):
        self.connect()
        for platform in ['fb','tw','vk','te']:
            for language in ['ru','uk']:
                mapke={}
                act=0
                with self.conn.cursor(name='name_of_cursora%s'%randint(0,999999)) as cursor:
                    cursor.execute("select keywords, date, language, text, username, lid, engagement, link from posts where keywords is not null and language = '%s' and date >= '%s' and date <= '%s' and platform = '%s' " % (language, start, end, platform))
                    cursor.itersize=40000
                    desc=None
                    print('exec')
                    for row in cursor:
                        act+=1
                        if act % 10000 == True:
                            print(act, platform, language, 'top100posts')
                        topics = self.getTopicsFromRealKeywords(row[0])

                        for t in set(topics):
                            t=t[0]
                            try:
                                mapke[t]
                            except KeyError:
                                mapke[t] = {}
                            try:
                                mapke[t][row[6]].append(row)
                            except KeyError:
                                mapke[t][row[6]] = [row]
                for topic, boo in mapke.items():
                    ct=0
                    out=[['post','topic','language','engagement_sum','username','text','date','link']]
                    for k,vq in sorted(boo.items(), reverse=True):
                        for v in vq:
                            ct+=1
                            if ct > 100:
                                break
                            out.append([v[5], topic, v[2],k, v[4], v[3], v[1], v[7]])
                        if ct > 100:
                            break
                    with codecs.open('%s/top100_%s_%s-%s_%s_%s.csv'% (self.outdir, language, platform, start, end, topic),'w', encoding='utf-8') as f:
                        w=csv.writer(f, delimiter=';')
                        w.writerows(out)
    def OldaddExtraFields(self, extrafields,out):
        if not extrafields == []:
            flds = ",".join(extrafields)
            newout=[]
            for o in out:
                qqqq="select %s from posts where lid = '%s'" % (flds, o['post'])
                self.cur.execute(qqqq)
                row = self.cur.fetchone()
                for ieq, f in enumerate(extrafields):
                    o[f] = row[ieq]
                newout.append(o)
            out=newout
        return out

    def topKeywordsOverTime(self, without_general=True):
        if self.kw == {}:
            self.read_keywords()
        skip=[]
        kw2topic={}
        for k,v in self.kw.items():
            if k =='General':
                for kk, vv in v.items():
                    skip += vv
            for kk, vv in v.items():
                for vvv in vv:
                    t="%s %s" % (k,kk)
                    kw2topic[vvv]=t
        self.connect()
        kct={}
        with self.conn.cursor(name='name_of_cursora%s'%randint(0,999999)) as cursor:
            cursor.execute("select platform, keywords, date, language from posts where keywords is not null and language in ('ru','uk')")
            cursor.itersize=40000
            desc=None
            for row in cursor:
                dt = row[2]
                pf=row[0]
                lan=row[3]
                try:
                    kct[pf]
                except KeyError:
                    kct[pf]={}
                try:
                    kct[pf][dt]
                except KeyError:
                    kct[pf][dt]={}
                for kw in row[1].split(','):
                    if without_general and kw in skip:
                        continue
                    try:
                        kct[pf][dt][lan]
                    except KeyError:
                        kct[pf][dt][lan]={}
                    try:
                        kct[pf][dt][lan][kw]+=1
                    except KeyError:
                        kct[pf][dt][lan][kw]=1
        out = [['date', 'platform', 'keyword','frequency','topic','language']]
        for pf,v in kct.items():
            for dt, vv in v.items():
                ct=0
                for lan, vvv in vv.items():
                    for w, val in sorted(vvv.items(), key=itemgetter(1), reverse=True):
                        ct+=1
                        if ct > 5:
                            break
                        try:
                            out.append([dt, pf, w, val, kw2topic[w],lan])
                        except KeyError as er:
                            print(er)
        with codecs.open('%s/top5_keywords_over_time.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)
    def compile_query(self, per, doreturn=False, qquery=None, top=0, extrafields=[], only_platform=None, splitper=None, fields_to_sum=[],fields_to_count=[], limit=None, only_lans=[], username=None, after_date=None, before_date=None):
        self.connect()
        print("Compile", per, only_platform)
        query = self.build_query(only_platform, only_lans, qquery, after_date, before_date, username)
        if not limit is None:
            query += ' limit %s' % limit
        print(query)
        dic={}
        cta=0
        self.cur.execute("select count(*) from posts")
        row = self.cur.fetchone()
        tot = row[0]
        with self.conn.cursor(name='name_of_cursora%s'%randint(0,999999)) as cursor:
            cursor.execute(query)
            cursor.itersize=10000
            desc=None
            for row in cursor:
                if desc is None:
                    desc = [a[0] for a in cursor.description]

                cta+=1
                if cta % 10000 == True:
                    print('--', "%s/%s" % (cta/10000, tot/10000), per)
                rowdict=dict(zip(desc,row))
                ok=True
                if not fields_to_sum == []:
                    for f in fields_to_sum:
                        if rowdict[f] is None:
                            ok=False
                if ok:
                    dic = self.process_row(rowdict, per, dic, fields_to_sum, fields_to_count)
        if doreturn:
            wo=[]
            for k,v in dic.items():
                a=k.split(self.seppy)
                az=dict(zip(per, a))
                az['v'] = v
                wo.append(az)
            return wo

        print(len(dic))
        lst = self.makeList(dic, top)
        print(len(lst))
        dd = self.splitPerList(per, splitper, lst)
        outs = self.makeOut( dd, per, fields_to_sum, fields_to_count, top)
        print("Ouuts", len(outs))
        for outy in outs:
            asfaefaw, out = outy
            out = self.addExtraFields( extrafields, out)

            if 'keyword' in out[0].keys():
                newout=[]
                for o in out:
                    o['theme'] = self.gettopic(o['keyword'])
                    newout.append(o)
                out=newout
            filename = "_".join(per) + '_'+str(only_platform) + '_'+str(asfaefaw)
            if username:
                filename += username
            if after_date:
                filename += after_date
            if not qquery is None:
                filename += qquery
            try:
                mapke={
                'ru': 'Russian',
                'uk': 'Ukrainian'
                }
                filename = out[0]['week'] + ' '+str(only_platform).upper() + ' - ' + \
                    'results for keywords for [' + out[0]['topic'] + '] in ' + mapke[out[0]['language']]
            except KeyError as ker:
                # print('KER', ker)
                if not only_lans == []:
                    filename += ' in '+ "-".join(only_lans)
            if not after_date is None:
                filename += '_after_%s' % after_date
            if not before_date is None:
                filename += '_before_%s' % before_date
            print("Write", filename)
            try:
                filename = "".join([a for a in filename if a in ('-','_') or a.isalnum()])
                self.writecsv(filename, out)
            except Exception as err:
                print(err)

    def writecsv(self, name, data):
        # if len(data) < 1:
            # return
        name=name.replace(':','_')
        fn=list(data[0].keys())
        #Convenience method to output csv
        print("Wrote", name)
        with codecs.open('%s/%s.csv'%(self.outdir,name),'w', encoding='utf-8') as f:
            w=csv.DictWriter(f, delimiter=';', fieldnames=fn)
            w.writeheader()
            w.writerows(data)

    def getdict(self, per, fields_to_sum=[], fields_to_count=[], extrafields=[], top=0,qquery=None,splitper=None):
        self.connect()
        if not extrafields==[] and not 'lid' in per:
            per.append('lid')
            if not fields_to_count == []:
                sys.exit("Correct query?")
            
        qf = fields_to_sum + fields_to_count + per
        
        query=self.apply_filters("SELECT " + ', '.join(qf) +" FROM posts")
        if 'urls'  in query:
            query += ' and urls is not null'
        #if not 'limit' in query:
        #    query += ' limit 100'
        dic = self.getDicFromDb(query, fields_to_sum, fields_to_count, per)

        out=[]
        hdz=None
        ct=0
        lst=[]
        if self.top and self.top > 0:
            for x in sorted(dic,key=lambda x:int(dic[x]['engagement']), reverse=True):
                lst.append([x, dic[x]])
        else:
            for k,v in dic.items():
                lst.append([k,v])
       
        
        dd=self.splitDict(lst, splitper)

        for asfaefaw, lst in dd.items():
            out=[]
            ct=0
            for ei, x in enumerate(lst):
                if ei % 1000 == True:
                    print('p',ei, len(lst), per)
                k=x[0]
                v=x[1]
                ct+=1
                if top > 0 and ct > top-1:
                    break
                
                di = self.fixFieldNames(per, v, fields_to_sum, fields_to_count, k, hdz)
                di = self.splitTopic(di)
                out.append(di)
            if len(out) == 0:
                continue
            if not extrafields == []:
                out=self.addExtraFields(extrafields, out)
          
            if 'keywords' in out[0].keys():
                newout=[]
                for o in out:
                    o['theme'] = self.getTopicsFromKeywords(o['keywords'])
                    newout.append(o)
                out=newout
            filename= self.getFileName(qquery, out[:1], per)
            self.writecsv(filename, out)


    def getFileName(self, qquery, out, per):

        if not qquery is None:
            filename += qquery
        # try:
        mapke={
            'ru': 'Russian',
            'uk': 'Ukrainian'
        }
        
        habba = per#+["%s %s" %(k,v) for k,v in self.filters.items()]
        habba = ','.join(habba)
        habba += ' - '+ '-'.join(["%s %s" %(k,v) for k,v in self.filters.items()])
        filename=habba
        try:
            filename = out[0]['week'] + ' '+str(self.filters['platform']).upper() + ' - ' + \
                'results for keywords for [' + out[0]['topic'] + '] in ' + self.filters['language']
        except KeyError as ker:
            pass
       
        if not self.top is None:
            filename = "top_%s_" % (self.top,) + filename
        return filename

    def addColumn(self):
        self.connect()
        self.cur.execute("alter table posts add column link text")
        self.conn.commit()
    def Oldwritecsv(self, name, data):
        if len(data) < 1:
            return
        name=name.replace(':','_')
        fn=list(data[0].keys())
        with codecs.open('%s/%s.csv'%(self.outdir,name),'w', encoding='utf-8') as f:
            w=csv.DictWriter(f, delimiter=';', fieldnames=fn)
            w.writeheader()
            w.writerows(data)
    
    def checkrussianhate(self):
        self.connect()
        self.cur.execute("select * from posts where lower(text) like '%#StopRussianHate%' limit 2")
        row=self.cur.fetchone()
        while row:
            print(row)
            row=self.cur.fetchone()

    def top_vk_ru_links(self):
        print('top_vk_ru_links')
        # Top most mentioned VK URLs from Russian posts (VK URL | count | topics of posts mentioning it)
        self.connect()
        self.cur.execute("select count(*) from urls where tld is null")
        row=self.cur.fetchone()
        #print(row)
        #sys.exit()
        urls={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            cursor.execute("select url, resolve from urls where tld = 'vk.com' or resolve like 'http://vk.com/%'  or resolve like 'https://vk.com/%' or url like 'https://vk.com/%' or url like 'http://vk.com/%'")
            for row in cursor:
                if not row[1] is None:
                    urls[row[0]] = row[1]
                else:
                    urls[row[0]] = row[0]

        print("Loaded")
        ct=0
        ay={}
        founds=0
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 5000
            cursor.execute("select urls, keywords from posts where urls is not null and keywords is not null and language in ('ru')")
            for row in cursor:
                ct+=1
                if founds > 30:
                    break
                if ct % 1000 == True:
                    print(ct, founds, 'top_vk_ru_links')
                topics = self.getTopicsFromKeywords(row[1].split(','))
                if not topics == []:
                    for u in row[0].split(','):
                        try:
                            vkurl = urls[u]
                            for topic in topics:
                                try:
                                    ay[topic]
                                except KeyError:
                                    ay[topic]={}
                                try:
                                    ay[topic][vkurl]+=1
                                except KeyError:
                                    ay[topic][vkurl] = 1
                            founds+=1
                        except KeyError:
                            pass
        out=[['topic', 'vkurl', 'count']]
        for topic, v in ay.items():
            for vkurl, ct in v.items():
                out.append([topic, vkurl, ct])
        with codecs.open('%s/vk_urls_per_topic_in_RU_posts.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)

    def getWeekDate(self, dt):
        year=dt.year
        wk = dt.isocalendar()[1]
        dt = "%s-W%s"% (year, wk)
        dt = str(datetime.strptime(dt + '-1', "%Y-W%W-%w"))[:10]
        return dt


    def debunk_posts(self):
        #top most engaged w/ posts from debunker Telegram channels
        channels=[]
        for m in os.listdir('debunkchans'):
            with codecs.open('debunkchans/%s' % m, 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=',')
                for row in r:
                    try:
                        if not row['author_username'] in channels:
                            channels.append(row['author_username'])
                    except KeyError:
                        if not row['name'] in channels:
                            channels.append(row['name'])
        self.connect()
        zib={}
        ct=0
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            print("Quey")
            cursor.execute("select lid, text, engagement, username, link from posts where platform = 'te' and engagement is not null")
            for row in cursor:
                ct+=1
                if ct % 5000 == True:
                    print(ct, 'debunk_posts')
                try:
                    zib[row[2]].append((row[0], row[1], row[3], row[4]))
                except KeyError:
                    zib[row[2]] = [(row[0], row[1],row[3], row[4])]
        ct=0
        out=[['engagement','id','text','username','link']]
        for k, v in sorted(zib.items(), reverse=True):
            for vv in v:
                ct+=1
                if ct > 100:
                    break
                out.append([k, vv[0], vv[1],vv[2], vv[3]])
        with codecs.open('%s/top_debunk_posts.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)
    
    def who_mentions_chan(self, channels=['nexta_live']):
        # who mentions Telegram “debunker” channels across our dataset.
        print('who_mentions_chan')
        self.connect()
        ct=0
        mentioners={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            print("Quey")
            cursor.execute("select urls, username, platform from posts where urls is not null limit 10000")
            for row in cursor:
                ct+=1
                if ct % 1000 == True:
                    print(ct, 'who_mentions_chan', len(mentioners))
                for c in channels:
                    isok=False
                    if 'https://t.me/s/%s' % c in row[0]:
                        isok=True
                        break
                if isok:
                    key = "%s_%s"% (row[2], row[1])
                    try:
                        mentioners[key]+=1
                    except KeyError:
                        mentioners[key]=1
        out=[['platform','username', 'count']]
        for k,v in mentioners.items():
            out.append(k.rsplit('_',1) + [v])
        with codecs.open('%s/debunk_mentioners.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)

    def bigrams_after(self, word=None, language='ru', usernames=[]):
        #Rankflow of bigrams for words after X per week
        stops = ['●','—', '...','»','—','«','|']
        stops=set(stops)
        tokenizer=TweetTokenizer()
        self.connect()
        word = word.lower()
        afters={}
        ct=0
        print('bigrams_after')
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 1000
            if usernames==[]:
                cursor.execute("select text, date from posts where language = %(l)s",{'l': language})
            else:
                cursor.execute("select text, date, username from posts where language = %(l)s",{'l': language})
            for row in cursor:
                if ct % 1000 == True:
                    print(ct, 'bigrams_after', len(afters))
                if not usernames == []:
                    if not row[2] in usernames:
                        continue
                txt=row[0].lower()
                if word in txt:
                    toks = tokenizer.tokenize(txt)
                    if word in toks:
                        week = self.getWeekDate(row[1])
                        toks = [t for t in toks if not t in stops and not t in punctuation]
                        captureNext=False
                        for t in toks:
                            if captureNext:
                                try:
                                    afters[week]
                                except KeyError:
                                    afters[week]={}
                                try:
                                    afters[week][t]+=1
                                except KeyError:
                                    afters[week][t] = 1
                                captureNext=False
                            if t == word:
                                captureNext=True
        out=[]
        for week,v in afters.items():
            l1=[week]
            l2=["%s_val"%week]
            for word2, val in sorted(v.items(), key=itemgetter(1), reverse=True):
                l1.append(word2)
                l2.append(val)
            out.append(l1)
            out.append(l2)
        fn='%s/rankflow_bigrams_after_%s_%s.csv' % (self.outdir,word,language)
        if not usernames == []:
            fn = '%s/rankflow_bigrams_after_%s_%s_debunkchannels.csv' % (self.outdir, word,language)
        with codecs.open(fn,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(zip_longest(*out))
    def telegramChannelsForTopics(self, topics=['Allegations of disinformation', 'Pro-war']):
        # A list of Telegram channels mentioned in RU pro-war material and RU allegations of disinformation 
        # (channel_entity | count | platform where they are mentioned)
        self.connect()
        self.read_keywords()
        ww=[]
        for topic in topics:
            ww += self.kw[topic]['Russian']
        ww=set(ww)
        booby=[]
        chanmap={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            cursor.execute("select url, resolve from urls where tld = 't.me' or resolve like 'http://t.me/%'  or resolve like 'https://t.me/%' or url like 'https://t.me/%' or url like 'http://t.me/%'")
            for row in cursor:
                # aq[row[0]] = row[1]
                if row[1] is not None:
                    we=row[1].split('?')[0]
                else:
                    we=row[0].split('?')[0]
                if '://t.me/' in we:
                    we=we.replace('https://t.me/','')
                    we=we.replace('http://t.me/','')
                    we = we.split('/')[0]
                    # print(we)
                    chanmap[row[0]] = we
        mapke={}
        ct=0
        founds=0
        print("Loaded")
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 5000
            cursor.execute("select urls, keywords, platform from posts where urls is not null and keywords is not null and language in ('ru')")
            for row in cursor:
                ct+=1
                if founds > 30:
                    break
                if ct % 1000 == True:
                    print(ct, founds, 'telegramChannelsForTopics')
                da = set(row[1].split(','))
                if len(da.intersection(ww)) > 0:
                    for u in row[0].split(','):
                        try:
                            te_chan = chanmap[u]
                            try:
                                mapke[row[2]]
                            except KeyError:
                                mapke[row[2]]={}
                            try:
                                mapke[row[2]][te_chan]+=1
                            except KeyError:
                                mapke[row[2]][te_chan]=1
                                founds+=1
                                
                        except KeyError:
                            pass
        out=[['channel', 'platform', 'count']]
        for pf, v in mapke.items():
            for techan, ct in v.items():
                out.append([techan, pf, ct])
        with codecs.open('/home/emillie/files/Telegram channels mentioned in RU pro-war material and RU allegations of disinformation.csv','w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)

    def updateKeywords(self, platform='te'):
        from nltk import TweetTokenizer
        self.read_keywords()
        # print(self.case_insensitive_keywords)
        kws=[]
        for k,v in self.kw.items():
            for kk, vv in v.items():
                kws+=vv
        kws.remove('мир')
        tokki=TweetTokenizer()
        self.connect()
        self.connect2()
        ct=0
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 5000
            print("Start query")
            cursor.execute("select text, keywords, lid from posts where platform = '"+platform+"'")
            for row in cursor:
                ct+=1
                if ct % 1000 == True:
                    print(ct, platform)
                if not row[0] is None:
                    toks=tokki.tokenize(row[0])
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
                    if not row[1] == ','.join(founds):
                        # print([row[1], ">>", ','.join(founds)])
                        self.cur2.execute("update posts set keywords = %(k)s where lid = %(l)s",{
                            'k': ','.join(founds),
                            'l': row[2]
                        })
                        self.conn2.commit()
                
    def geturls2resolve(self, topics=['Pro-war','Allegations of disinformation']):
        print('geturls2resolve')
        from urls import urlResolver
        from tld import get_tld

        self.read_keywords()
        # print(self.kw.keys())
        # A co-URL network for URL domains mentioned in RU allegations of disinformation.
        ww=[]
        for topic in topics:
            ww += self.kw[topic]['Russian']
        ww=[a.replace('*','').lower() for a in ww]
        # print(ww)
        ww=set(ww)
        self.connect()
        ct=0
        ur={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 5000
            print("Start query")
            cursor.execute("select urls, keywords from posts where urls is not null and keywords is not null and language = 'ru' ")
            for row in cursor:
                ct+=1
                if ct % 1000 == True:
                    print(ct)
                kws = set(row[1].split(','))
                if len(kws.intersection(ww)) > 0:
                    for q in row[0].split(','):
                        ur[q]=1
        print("Load urls...", len(ur))
        self.cur.execute("select url, tld from urls where tld is not null or resolve = 'ERROR'")
        row=self.cur.fetchone()
        while row:
            try:
                del ur[row[0]]
            except KeyError:
                pass
            row=self.cur.fetchone()
        resolver = urlResolver()
        ks=list(ur.keys())
        shuffle(ks)
        print("Start resolve", len(ks))
        for i,u in enumerate(ks):
            print(i, len(ks), u)
            resolve, tld = resolver.doresolve(u)


    def resolveUnresolved(self, limit=100):
        print('resolveurls')
        from urls import urlResolver
        resolver = urlResolver()

        self.connect()
        self.connect2()
        ct=0
        ur={}
        self.cur.execute("select count(*) from urls where resolve is null")
        ra = self.cur.fetchone()
        tot=ra[0]
        with open('/var/scripts/log/resolve.log','w') as f:
            self.cur.execute("select url from urls where resolve is null order by random() limit %s" % int(limit))
            row=self.cur.fetchone()
            while row:
                ct+=1
                if ct % 10 == True:
                    print(ct, tot)
                    f.write("Resolved %s" % ct)
                try:
                    a,b =resolver.doresolve(row[0].split("\n")[0])
                    if a is not None:
                        self.cur2.execute("update urls set resolve = %(r)s, tld = %(t) where url = %(u)s and resolve is null", {'r':a, 't':b, 'u': row[0]})
                        self.conn2.commit()
                except Exception as er:
                    if 'invalid session id' in str(er):
                        break
                row=self.cur.fetchone()
            f.write("Done")

    def dumpUrls(self):
        self.connect()
        a = pickle.load(open('slow_resolves.p','rb'))
        #a = pickle.load(open('resolves.p','rb'))
        ct=0
        for k,v in a.items():
            if not v[0] is None:
                ct+=1
                if not v[1] is None:
                    self.cur.execute("update urls set resolve = %(r)s, tld = %(t)s where url = %(u)s and resolve is null", {
                        'u': k,
                        't':v[1],
                        'r':v[0]
                    })
                else:
                    self.cur.execute("update urls set resolve = %(r)s where url = %(u)s and resolve is null", {
                        'u': k,
                        'r':v[0]
                    })
                if ct % 100 ==True:
                    self.conn.commit()
        if ct > 0:
            self.conn.commit()
        out=[]
        print("Updated",ct)
        with self.conn.cursor(name='name_of_cursora%s'%randint(0,999999)) as cursor:

            cursor.itersize = 20000
            cursor.execute("select url from urls where resolve is null")
            for row in cursor:
                out.append([row[0], ''])
        with codecs.open('urls2.csv','w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)

    def quickresolve(self, limit=100, iters=3):
        iters = int(iters)
        limit = int(limit)
        import requests
        from tld import get_tld
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        self.connect()
        self.connect2()
        self.cur2.execute("select count(*) from urls where resolve is null")
        raw = self.cur2.fetchone()
        tot = raw[0]/1000
        try:
            timeouts=pickle.load(open('quicktimeouts.p','rb'))
        except Exception as err:
            print(err)
            timeouts={}
        ct=0
        skips=0
        errs=0
        upd=0
        changed=False
        for i in range(1, iters+1):
            with self.conn.cursor(name='name_of_cursora%s'%randint(0,999999)) as cursor:

                cursor.itersize = 1000
                cursor.execute("select url from urls where resolve is null order by random() limit %s" % int(limit))
                dones={}
                for row in cursor:
                    #don't do the same domain twice in order not to 
                    ct+=1
                    if ct % 10 == True:
                        print(i, ct, "U:%s S:%s E:%s" % (upd, skips, errs), tot)
                    roq = row[0].split('/')
                    if len(roq) < 3:
                        skips+=1
                        continue
                    try:
                        if randint(0,10) == 5:
                            dones[roq[2]]
                            skips+=1
                            continue
                    except KeyError:
                        dones[roq[2]] = 1
                    try:
                        timeouts[roq[2]]
                        skips+=1
                        continue
                    except KeyError:
                        pass
                    #try:
                    if True:
                        qqq = row[0].split("\n")[0]
                        try:
                            res = requests.head(qqq, allow_redirects=True, timeout=5, verify=False)
                        except Exception as err:
                            errs+=1
                            isrealerr=False

                            if 'No address associated with hostname' in str(err):
                                isrealerr=True
                            elif 'No connection adapters were found' in str(err):
                                isrealerr=True
                            elif 'Connection refused' in str(err):
                                isrealerr=True
                            elif 'Name or servie not known' in str(err):
                                isrealerr = True
                            if 'timed out' in str(err) or 'reset by peer' in str(err) or 'without response' in str(err) or 'Temporary failure in name resolution' in str(err):
                                timeouts[roq[2]] = 1
                            if isrealerr:
                                changed=True
                                self.cur2.execute("update urls set resolve = %(e)s where url = %(u)s", {'u': row[0], 'e': str(err)})
                            print((isrealerr, err, roq[2]))
                            continue
                        res = res.url
                        if res:
                            t=None
                            try:
                                t = get_tld(res, as_object=True)
                                t = "%s.%s" % (t.domain, t.tld)
                            except Exception as rerr:
                                print(rerr)

                            self.cur2.execute("update urls set resolve = %(r)s, tld= %(t)s where url = %(u)s", {
                                't': t,
                                'r': res,
                                'u':row[0]
                                })
                            upd+=1
                            changed=True
                        if ct % 10 == True and changed:
                            self.conn2.commit()
                            changed=False
                #except Exception as err:
                #    print(err)
        if changed:
            self.conn2.commit()
        try:
            p2 = pickle.load(open('quicktimeouts.p','rb'))
            for k,v in p2.items():
                timeouts[k] = v
        except Exception as err:
            print(err)
        pickle.dump(timeouts, open('quicktimeouts.p','wb'))
        print("Done!")


    def insertMissingUrls(self, limit=10):
        limit = int(limit)
        self.connect()
        self.connect2()
        aq={}
        us={}
        adds=0
        known={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            cursor.execute("select url from urls")
            for row in cursor:
                known[row[0]] = 1
        print("Loaded")
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            cursor.execute("select urls from posts where urls is not null")
            for row in cursor:
                for u in row[0].split(','):
                    u = u[:2700]#too long for pkey otherwise
                    try:
                        known[u]
                        continue
                    except KeyError:
                        pass
                    us[u]=1
        print("Loaded 2")
        ct=0
        for k, v in us.items():
            self.cur.execute("select url from urls where url = %(u)s",{'u':k})
            row = self.cur.fetchone()
            if not row:
                ct+=1
                self.cur2.execute("insert into urls (url) values (%(u)s)", {'u':k})
                adds+=1
                if adds % 1000 == True:
                    print(ct, len(us))
                    self.conn2.commit()
            if adds > limit:
                break
        if adds > 0:
            self.conn2.commit()
        print("Added %s urls" % adds)
    def co_url_disinfo(self, topic='Allegations of disinformation'):
        print('co_url_disinfo', topic)
        from urls import urlResolver
        from tld import get_tld

        self.read_keywords()
        # print(self.kw.keys())
        # A co-URL network for URL domains mentioned in RU allegations of disinformation.
        ww = self.kw[topic]['Russian']
        ww=[a.replace('*','').lower() for a in ww]
        # print(ww)
        ww=set(ww)
        # sys.exit()
        self.connect()
        self.connect2()
        aq={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            cursor.execute("select url, tld from urls where tld is not null or resolve = 'ERROR'")
            for row in cursor:
                aq[row[0]] = row[1]
        print("Loaded", len(aq))
        resolver = urlResolver()
        G=nx.Graph()
        ct=0
        resolves=0
        inserts=0
        founds=0
        # wappi=' and '
        # for www in ww:
            # wappi += " keywords like '%" + www +"%' and "
        # wappi = wappi[:4]
        kasji={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 1000
            cursor.execute("select urls, keywords from posts where urls like '%,%' and keywords is not null and language in ('ru') ")
            for row in cursor:
                ct+=1
                # if founds > 100:
                    # break
                if ct % 100 == True:
                    print(ct, resolves, founds,'co_url_disinfo', topic)
                da = set(row[1].split(','))
                if len(da.intersection(ww)) > 0:
                    tldz=[]
                    for u in row[0].split(','):
                        resolve=None
                        try:
                            tld=kasji[u]
                        except KeyError:
                            try:
                                tld=aq[u]
                            except KeyError:
                                resolves+=1
                                resolve, tld = resolver.doresolve(u)
                                # self.cur2.execute("select url from urls where url = %(u)s", {'u': u})
                                # rrow = self.cur2.fetchone()
                                # if not rrow:
                                    # self.cur2.execute("insert into urls (url, resolve, tld) values (%(u)s, %(r)s, %(t)s)",{
                                        # 'u': u,
                                        # 'r': resolve,
                                        # 't': tld
                                    
                                    # })
                                    # self.conn2.commit()
                                    # inserts+=1
                                # else:
                                    # self.cur2.execute("update urls set resolve = %(r)s, tld = %(t)s where url = %(u)s", {
                                        # 'u': u,
                                        # 'r': resolve,
                                        # 't': tld
                                    # })
                                    # self.conn2.commit()
                            if tld is None:
                                tld=u
                                try:#hacky hacky
                                    if not resolve is None:
                                        t = get_tld(resolve, as_object=True)
                                    else:
                                        t = get_tld(u, as_object=True)
                                    tld = "%s.%s" % (t.domain, t.tld)
                                except Exception as err:
                                    print(err)
                                    tld=u

                            tldz.append(tld)
                            kasji[u] = tld
                    tldz = [t for t in tldz if not tld is None]
                    if len(tldz) > 1:
                        founds+=1
                    for c in combinations(tldz,2):
                        a=c[0]
                        b=c[1]
                        if G.has_edge(a,b):
                            G[a][b]['weight']+=1
                        elif G.has_edge(b,a):
                            G[b][a]['weight']+=1
                        else:
                            G.add_edge(a, b, weight=1)
        nx.write_gexf(G, '%s/co-URL network for URL domains mentioned in RU %s.gexf' % (self.outdir,topic))
    def nw_and_csv_for_topic_urlcounts(self):
        #mediadiet_urlcounts_Allegations of disinformation RU and UK — CSV and gephi

        self.connect()
        uu={}
        binz={}
        with self.conn.cursor(name='name_of_cursor_%s' % randint(0,9999999)) as cursor:
            cursor.itersize = 10000
            cursor.execute("select urls, keywords, username, platform from posts where urls is not null and keywords is not null and language in ('uk','ru') ")
            for row in cursor:
                ok=False
                topics = self.getTopicsFromKeywords(row[1].split(','))
                for t in topics:
                    if 'Allegations of disinformation' in t:
                        ok=True
                if ok:
                    # print(topics)
                    usr = "%s_%s"% (row[2], row[3])
                    try:
                        binz[usr] += row[0].split(',')
                    except KeyError:
                        binz[usr] = row[0].split(',')
                    for u in row[0].split(','):
                        try:
                            uu[u]+=1
                        except KeyError:
                            uu[u]=1
        out=[]                    
        for k,v in sorted(uu.items(), key=itemgetter(1), reverse=True):
            out.append([k,v])
        with codecs.open('%s/mediadiet_urlcounts_Allegations of disinformation RU and UK.csv'%self.outdir,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)
        G=nx.DiGraph()
        for usr, urls in binz.items():
            c=Counter(urls)
            for k,v in c.items():
                if G.has_edge(usr, k):
                    G[usr][k] += v
                else:
                    G.add_edge(usr, k, weight=v)
                G.nodes[usr]['type'] = 'User'
                G.nodes[k]['type'] = 'Url'
        nx.write_gexf(G, '%s/mediadiet_urlcounts_Allegations of disinformation RU and UK.gexf' % self.outdir)
        
    def HashOrigins(self):
        from nltk import TweetTokenizer
        from string import punctuation
        from gensim.models import Phrases
        t=TweetTokenizer()
        self.connect()
        d2='2022-03-02'
        lan='ru'
        self.cur.execute("select lower(hashtags) from posts where hashtags is not null and date = '"+d2+"'and language = '"+lan+"' limit 1")
        ct=0
        tags={}
        while True:
            rows = self.cur.fetchmany(1000)
            if len(rows) > 0:
                for row in rows:
                    ct+=1
                    if ct % 1000 == True:
                        print('L',ct)
                    taggies = row[0].split(',')
                    for t in taggies:
                        tags[t] = 1
            
            else:
                break
        
        for t, b in tags.items():
            print(t)
            self.cur.execute("select username, platform from posts where lower(hashtags) like %(t)s order by date asc limit 1", {'t': '%,'+t+',%'})
            row=self.cur.fetchone()
            print(row)
            
            

    def DeltaWords(self, d1='2022-02-02', d2='2022-03-20', lan='uk'):
        from nltk import TweetTokenizer
        from string import punctuation
        from gensim.models import Phrases
        t=TweetTokenizer()
        self.connect()
        self.cur.execute("select lower(text) from posts where date = '"+d1+"' and language = '"+lan+"'")
        early={}
        ct=0
        texts=[]
        while True:
            rows = self.cur.fetchmany(1000)
            if len(rows) > 0:
                for row in rows:
                    ct+=1
                    if ct % 1000 == True:
                        print('E',ct)
                    texts.append([q for q in t.tokenize(row[0]) if not q in punctuation])
                    # for word in t.tokenize(row[0]):
                        # try:
                            # early[word]+=1
                        # except KeyError:
                            # early[word]=1
            else:
                break
        textslate=[]
        self.cur.execute("select lower(text) from posts where date = '"+d2+"'and language = '"+lan+"'")
        late={}
        ct=0
        while True:
            rows = self.cur.fetchmany(1000)
            if len(rows) > 0:
                for row in rows:
                    ct+=1
                    if ct % 1000 == True:
                        print('L',ct)
                    textslate.append([q for q in t.tokenize(row[0]) if not q in punctuation])
                    # for word in t.tokenize(row[0]):
                        # try:
                            # late[word]+=1
                        # except KeyError:
                            # late[word]=1
            else:
                break
        print("Training...")
        bigram = Phrases(texts + textslate, min_count=3, threshold=2)
        print("Trained!")
        for e in texts:
            a = [z for z in bigram[e] if '_' in z]
            for word in a:
                try:
                    early[word]+=1
                except KeyError:
                    early[word]=1
        for e in textslate:
            a = [z for z in bigram[e] if '_' in z]
            for word in a:
                try:
                    late[word]+=1
                except KeyError:
                    late[word]=1

        diffs={}
        for k,v in late.items():
            try:
                ev = early[k]
            except KeyError:
                ev=0
            difval = v-ev
            try:
                diffs[difval].append(k)
            except KeyError:
                diffs[difval] = [k]
        ct=0
        out=[['count_difference','phrase', d1+'_freq', d2+'_freq']]
        for k,v in sorted(diffs.items(), reverse=True):
            for vv in v:
                try:
                    earl = early[vv]
                except KeyError:
                    earl=0
                
                lat = late[vv]
                out.append([k,vv, earl, lat])
            ct+=1
            if ct > 200:
                break
        with codecs.open('%s/delta_%s_%s_%s.csv' % (self.outdir, d1,d2,lan),'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)
