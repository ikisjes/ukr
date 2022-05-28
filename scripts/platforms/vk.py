from abstractplatform import abstractPlatform
from datetime import datetime
class Vky(abstractPlatform):
    DATASETDIR='/var/scripts/datasets/general_queries_got'
    platform = 'vk'
    def __init__(self):
        super().__init__()


    def getUserId(self, row):
        return row['from_id']
    def updateUserNamesFromFiles(self):
        import vk_api
        authors = {}
        self.connect()
        self.cur.execute("select uid from vk_users")
        row= self.cur.fetchone()
        while row:
            authors[row[0]] = 1
            row= self.cur.fetchone()
        self.cur.execute("select gid from vk_groups")
        row= self.cur.fetchone()
        while row:
            authors[-1*row[0]] = 1
            row= self.cur.fetchone()
        for x in os.listdir('datasets/general_queries_got'):
            dta = pickle.load(open('datasets/general_queries_got/%s' % x, 'rb'))
            for xx in dta:
                #from_id = author, owner_id = wall where it's posted
                uid = int(row['from_id'])
                try:
                    authors[uid]
                except KeyError:
                    authors[uid]=0
        vk_session = vk_api.VkApi('+31643900251', 'iv123an')
        vk_session.auth()
        vk = vk_session.get_api()
        tot=len(authors)
        ct=0
        for uid, q in authors.items():
            ct+=1
            if ct % 1000 == True:
                print(ct, tot)
                if changed:
                    self.conn.commit()
                    changed=False
            if q == 0:
                print("Get", uid)
                if uid < 0:
                    group = vk.groups.getById(group_id=abs(uid))
                    self.cur.execute("insert into vk_groups (gid, name, type, is_advertiser, is_closed) values (%(id)s, %(name)s, %(type)s, %(is_advertiser)s, %(is_closed)s", {
                        group
                    })
                    changed=True
                else:
                    user = vk.users.get(user_id=uid)
                    self.cur.execute("insert into vk_users (uid, first_name, last_name, can_access_closed, is_closed) values (%(id)s, %(first_name), %(last_name)s, %(can_access_closed)s, %(is_closed)s);", user)
                    changed=True
        if changed:
            self.conn.commit()
    #def getUserId(self, row):
        
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

    def getId(self, row):
        #the ID field is NOT a post id!
        return self.getLink(row)
        #return "%s_%s_%s"% (row['id'], row['date'], row['owner_id'])
    def isValidFile(self, x):
        if x.endswith('.p'):
            return True
        return False
    def getUser(self, row):
        return row['from_id']
    def getDate(self, row):
            
        return datetime.utcfromtimestamp(row['date']).date()
    def getEngagement(self, row):
        score=0
        for xx in [ 'likes', 'reposts', 'views']:
            try:
                score+= int(row[xx]['count'])
            except KeyError:
                pass
        return score
