from abstractplatform import abstractPlatform
import numpy as np
from datetime import datetime
import sys

class Twittery(abstractPlatform):
    platform = 'tw'
    def __init__(self):
        super().__init__()
    def getLink(self, row):
        return 'https://twitter.com/a/status/%s'% self.getId(row)
    def getId(self, row):
        try:
            return row['id_str']
        except KeyError:
            return str(row['id'])
    def getUserId(self,row):
        if not row['author_id'].isnumeric():
            print(row)
            print("Nonnumeric aid")
            sys.exit()
        return row['author_id']
    def getUser(self,row):
        try:
            return row['author_username']
            #return row['author_id']
        except KeyError:
            try:
                return row['screen_name']
            except KeyError:
                try:
                    return row['author_fullname']
                except KeyError:
                    try:
                        return row['author_user']['username']
                    except KeyError:
                        try:
                            return row['username']
                        except KeyError:
                            try:
                                return row['author_id']
                            except KeyError:
                                print("Twitter - no user field found")
                                print(row)
                                print(row.keys())
                                raise Exception
    def getLanguage(self, row):
        try:
            return row['language_guess']
        except KeyError:
            return super(Twittery, self).getLanguage(row)

    def isValidFile(self, x):
        if 'Twitter' in x and (x.endswith('.csv') or x.endswith('.njson')):
            return True
        return False
    def getDate(self, row):
            try:
                return datetime.strptime(row['created_at'][:10], "%Y-%m-%d").date()
            except KeyError:
                try:
                    if row['timestamp'] is None:
                        return
                    return datetime.strptime(row['timestamp'][:10], "%Y-%m-%d").date()
                except ValueError:
                    return
    def getEngagement(self, row):
        try:
            try:
                return np.sum(list(row['public_metrics'].values()))
            except AttributeError:
                row['public_metrics']=eval(row['public_metrics'])
                return np.sum(list(row['public_metrics'].values()))
        except KeyError:
            score=0
            try:
                for b in ['retweet_count', 'reply_count', 'like_count', 'quote_count']:
                    try:
                        try:
                            score += int(row[b])
                        except TypeError:
                            pass
                    except ValueError:
                        pass
            except KeyError:
                sys.exit('Telegram file?')
                try:
                    return row['views']
                except KeyError:
                    print(row)
                    print("Twitter missing engagement")
                    print(row.keys())
                    sys.exit()
            return score


