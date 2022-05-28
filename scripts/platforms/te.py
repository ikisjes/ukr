from abstractplatform import abstractPlatform
from datetime import datetime
import codecs, csv
class Telegramy(abstractPlatform):
    languagemap={}
    platform='te'
    def __init__(self):
        with codecs.open('/var/scripts/platforms/list of telegram channels.csv','r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                lan=None
                if row['Language'] == 'Russian':
                    lan='ru'
                elif row['Language'] == 'Ukrainian':
                    lan='uk'
                
                if not lan is None:
                    self.languagemap[row['Name']] = lan
        super().__init__()
    
    def getId(self, row):

        return self.getLink(row)
    def getText(self, row):
        try:
            return row['message']
        except KeyError:
            return row['body']
    def getLink(self,row):
        try:
            return 'https://t.me/%s/%s' % (row['search_entity'], row['id'])
        except KeyError:
            try:
                return 'https://t.me/%s/%s' % (row['author_username'], row['id'])
            except KeyError:
                try:
                    return row['link']
                except KeyError:
                    for k,v in row.items():
                        print([k,v])
                    print(row.keys())
                    print('no username')
                    sys.exit()

    def getUser(self,row):
        #post_author seems None
        try:
            return row['search_entity']
        except KeyError:
            try:
                return row['author_username']
            except KeyError:
                try:
                    #hacky - get the username from the link
                    nm = row['link'].split('/')[3]
                    return nm
                except KeyError:
                    for k,v in row.items():
                        print([k,v])
                    print(row.keys())
                    print('no username 2')
                    sys.exit()

        
    def getLanguage(self,row):
        try:
            return self.languagemap[self.getUser(row)]
        except KeyError:
            return super(Telegramy, self).getLanguage(row)    
    def isValidFile(self, x):
        if 'Telegram' in x and x.endswith('.csv'):
            return True
        return False
    def getUserId(self, row):
        user_id=None
        try:
            user_id = row["_sender"]["id"]
        except KeyError:
            pass
        return user_id
        #if not row['sender_id'] is None:
        #    return row['sender_id']
        #return row['author']
    def getDate(self, row):
        try:
            return datetime.fromtimestamp(row["date"]).strftime("%Y-%m-%d")
        except KeyError:
            return datetime.strptime(row['timestamp'][:10], "%Y-%m-%d").date()
    def getEngagement(self, row):
        return row['views']
