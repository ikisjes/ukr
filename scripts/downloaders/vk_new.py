import vk_api, os, sys,pickle, time, psycopg2, codecs,csv

sys.path.append('/var/scripts')
#from keywords import keywords
from random import shuffle
from datetime import timedelta, datetime, date
import pymongo
from functions import get_keywords
keywords = get_keywords()

#RUN AS IVAN< NOT ROOT
client = pymongo.MongoClient("mongodb://localhost:27017")

qdb = client['queries']
qcoll = qdb['vkqueries']

db = client['original']
collection = db['vk']

archive = client['archive']
arcollection = archive['vk']


class getVK():
    PHONE=''
    PASS=''
    #start_date = date(2022, 1, 1)
    start_date = date(2022, 3, 29)
    end_date = datetime.now().date() - timedelta(days=1)
    keywords=[]
    api=None
    cur=None
    conn=None
    toget=[]
    known_links={}
    def __init__(self):
        self.get_keywords()
    def checkKnown(self):
        print("Checking known - in case something crashed")
        for post in collection.find():
            self.known_links[post['link']] = 1
        for post in arcollection.find():
            self.known_links[post['link']] = 1
        print("Done!")
    def getLink(self, row):
        try:
            if row['post_type'] == 'reply':
                return 'https://vk.com/wall-%s_%s?%s'  % (str(row['owner_id']).replace('-',''), str(row['post_id']), str(row['id']))
            elif row['post_type'] == 'post':
                return 'https://vk.com/wall-%s_%s' % (str(row['owner_id']).replace('-',''), str(row['id']))
            else:
                sys.exit("Unknown post type: %s" % row['post_type'])
        except KeyError:
            if row['is_deleted']:
                if 'post_id' in row.keys():
                    return 'https://vk.com/wall-%s_%s?%s'  % (str(row['owner_id']).replace('-',''), str(row['post_id']), str(row['id']))
                else:
                    return 'https://vk.com/wall-%s_%s' % (str(row['owner_id']).replace('-',''), str(row['id']))
    def connect(self):
        if self.cur is None:
            print("Connecting...")
            self.conn=psycopg2.connect(database="ukraine", user="postgres", password="root",port=5432)
            self.conn.set_client_encoding('UTF8')
            self.cur = self.conn.cursor()
    def get_keywords(self):
        with codecs.open('/var/scripts/keywords.csv','r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter='\t')
            for row in r:
                if row['Keyword (please watch out for stemming *)'].strip():
                    if not row['Keyword (please watch out for stemming *)'] == 'мир':

                    # try:
                        # self.kw[row['Theme']]
                    # except KeyError:
                        # self.kw[row['Theme']]={}
                    # try:
                        # self.kw[row['Theme']][row['Language']]
                    # except KeyError:
                        # self.kw[row['Theme']][row['Language']]=[]
                    # self.kw[row['Theme']][row['Language']].append(row['Keyword (please watch out for stemming *)'])
                        self.keywords.append(row['Keyword (please watch out for stemming *)'])

        #for k,v in keywords.items():
        #    for kk, vv in v.items():
        #        vv = vv.split(',')
        #        vv = [q.strip().lower().replace('*','') for q in vv if q.strip()]
        #        self.keywords+=vv
        self.keywords = [q.strip().lower().replace('*','') for q in keywords if q.replace('*','').strip()]
        self.keywords = list(set(self.keywords))
    def start_api(self):
        vk_session = vk_api.VkApi(self.PHONE, self.PASS)
        vk_session.auth(token_only=True)
        self.api = vk_session.get_api()
        print("ok?")

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)


    def get_queries_to_get(self):
        wants=[]
        known ={}
        for post in qcoll.find():
            known["%s_%s"% (post['q'], post['dt'])] = 1

        for single_date in self.daterange(self.start_date, self.end_date):
            for queryword in self.keywords:
                key = "%s_%s"% (queryword, single_date)
                try:
                    known[key]
                    continue
                except KeyError:
                    known[key]=1
                    wants.append((queryword, single_date))
        shuffle(wants)
        self.toget = wants
    def startDownload(self):
        self.checkKnown()
        self.get_queries_to_get()
        print(len(self.toget), 'queries left')
        for i, query in enumerate(self.toget):
            queryword, dt = query
            print('Get', '%s/%s' % (i, len(self.toget)), query)
            self.getData(queryword, dt)
            qcoll.insert_one({'q': queryword, 'dt': str(dt)})
        print("Done getting data (for now)!")
    def realget(self, queryword, mindat, maxdat, nextfrom = None):
        fields=['city', 'connections', 'counters', 'country', 'domain', 'exports', 'followers_count', 'has_photo', 'home_town', 'interests', 'is_no_index', 'first_name','last_name', 'deactivated', 'is_closed', 'military','nickname', 'personal', 'photo_50','relatives', 'schools','screen_name', 'sex', 'timezone', 'verified', 'wall_default']
        try:
            if nextfrom is None:
                data = self.api.newsfeed.search(v=5.81,q=queryword,  count=200, start_time = mindat, end_time = maxdat, extended=1, fields=fields)
            else:
                data = self.api.newsfeed.search(v=5.81,q=queryword, start_from = nextfrom, count=200, start_time = mindat, end_time = maxdat, extended=1, fields=fields)
        except Exception as err:
        # except vk.exceptions.VkAPIError as err:
            print(err)
            sys.exit()
            # time.sleep(5)
            
            # data = self.api.newsfeed.search(v=5.81,q=queryword,  count=200, start_time = mindat, end_time = maxdat, extended=1, fields=fields)
        return data

    def getData(self, queryword, single_date):  
        md = single_date.strftime("%Y-%m-%d")
        md2 = (single_date + timedelta(days=1)).strftime("%Y-%m-%d")
        mindat = datetime.strptime(md, "%Y-%m-%d").timestamp()
        maxdat = datetime.strptime(md2, "%Y-%m-%d").timestamp()  
        data = self.realget(queryword, mindat, maxdat)
        # try:
            # data = self.api.newsfeed.search(v=5.81,q=queryword,  count=200, start_time = mindat, end_time = maxdat, extended=1)
        # except Exception as err:
        # # except vk.exceptions.VkAPIError as err:
            # print(err)
            # sys.exit()
            # time.sleep(5)
            # data = self.api.newsfeed.search(v=5.81,q=queryword,  count=200, start_time = mindat, end_time = maxdat, extended=1)
        out=[]
        hasnext = True
        if not 'next_from' in data.keys():
            hasnext = False
        for itm in data['items']:
            out.append(itm)
        nextfrom = ''
        if data['count'] < 199:
            hasnext=False
        pct=0
        while hasnext:
            time.sleep(1)
            hasnext=False
            if data['count'] > 199:
                hasnext=True
            time.sleep(2)
            pct+=1
            print('-------', pct, len(out), single_date, queryword)
            for itm in data['items']:
                if not itm in out:
                    out.append(itm)
            if not 'next_from' in data.keys():
                hasnext = False
            else:
                nextfrom = data['next_from']
                data = self.realget(queryword, mindat, maxdat,nextfrom)
                # try:
                    # data = self.api.newsfeed.search( start_from = nextfrom,v=5.81,q=queryword, count=200, start_time = mindat, end_time = maxdat, extended=1)
                # except vk.exceptions.VkAPIError as err:
                    # print(err)
                    # time.sleep(5)
                    # data = self.api.newsfeed.search( start_from = nextfrom,v=5.81,q=queryword, count=200, start_time = mindat, end_time = maxdat, extended=1)
        print(datetime.strptime(md, "%Y-%m-%d").date(), queryword, len(out))
        for o in out:
            link = self.getLink(o)
            try:
                self.known_links[link]
            except KeyError:
                o['keyword'] = queryword
                o['querydate'] = str(datetime.strptime(md, "%Y-%m-%d").date())
                o['link'] = link
                collection.insert_one(o)
                #because a post may reoccur in different queries
                self.known_links[link]=1
        #pickle.dump(out, open("general_queries_got/%s_%s.p" % (queryword, single_date), 'wb'))

        time.sleep(3)
        
if __name__ == '__main__':
    os.chdir('/var/scripts/downloaders')
    vk = getVK()
    vk.start_api()
    q2g = vk.startDownload()
