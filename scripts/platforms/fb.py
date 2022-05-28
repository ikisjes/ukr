from abstractplatform import abstractPlatform
from datetime import datetime

class Facebooky(abstractPlatform):
    platform = 'fb'

    def __init__(self):
        super().__init__()
    def getLink(self, row):
        return row['URL']
    def getId(self, row):
        return "%s_%s"% (row['Facebook Id'], row['Post Created'])
    def isValidFile(self, x):
        if 'Facebook' in x and x.endswith('.csv'):
            return True
        return False
    def getUserId(self, row):
        return
    def getDate(self, row):
        if row['Post Created'] is None:
            return
        try:
            return datetime.strptime(row['Post Created'][:10], "%Y-%m-%d").date()
        except ValueError:
            return
        
    def getUser(self, row):
        try:
            return row['Group Name']
        except KeyError:
            try:
                return row['\ufeffGroup Name']
            except KeyError:
                try:
                    return row['\ufeffPage Name']
                except KeyError:
                    return row['\ufeffPage Name']
    def getEngagement(self, row):
        return row['Total Interactions'].replace(',','.')
    def getLocus(self, row):
        lang = self.getLanguage(row)
        place = None
        try:
            place = row['Page Admin Top Country']
        except KeyError:
            pass
        return (lang, place)
    def getText(self,row):
        return row['Message']
        

