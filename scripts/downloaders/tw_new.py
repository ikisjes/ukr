from functions import *
import time, sys,requests
from datetime import date, datetime, timedelta
import copy
import pymongo
from random import shuffle
class Tweeter:
    bearer_token = ''
    client=None
    qcoll=None
    collection=None
    arcollection=None
    url = "https://api.twitter.com/2/tweets/search/all"


    known_links={}
    start_date = date(2022, 3, 29)
    end_date = datetime.now().date() - timedelta(days=1)
    keywords = []
    kwlanmap = {}
    
    expansions='attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id' 
    mediafields = 'duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics,alt_text'
    placefields = 'contained_within,country,country_code,full_name,geo,id,name,place_type'
    tweetfields='attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld'
    userfields='created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld'
    previous_request=None
    def __init__(self):
        k = get_keywords(w_language=True)
        for x in k:
            l=None
            if x[0] == 'Russian':
                l='ru'
            elif x[0] == 'Ukrainian':
                l='uk'
            elif x[0] == 'N.A.':
                l=None
            else:
                print('>>>>>>>>>>>>',x)
                sys.exit("Unkonwn language")
            self.kwlanmap[x[1]] = l
            self.keywords.append(x[1])
        client = pymongo.MongoClient("mongodb://localhost:27017")
        qdb = client['queries']
        self.qcoll = qdb['twqueries']

        db = client['original']
        archive = client['archive']
        self.collection = db['tw']
        self.arcollection = archive['tw']
        self.checkKnown()


    def enrich_tweet(self, tweet, users, media, polls, places, referenced_tweets, missing_objects):
        """
        Enrich tweet with user and attachment metadata
        Twitter API returns some of the tweet's metadata separately, as
        'includes' that can be cross-referenced with a user ID or media key.
        This makes sense to conserve bandwidth, but also means tweets are not
        'standalone' objects as originally returned.
        However, for processing, making them standalone greatly reduces
        complexity, as we can simply read one single tweet object and process
        that data without worrying about having to get other data from
        elsewhere. So this method takes the metadata and the original tweet,
        splices the metadata into it where appropriate, and returns the
        enriched object.
        /!\ This is not an efficient way to store things /!\ but it is more
        convenient.
        :param dict tweet:  The tweet object
        :param list users:  User metadata, as a list of user objects
        :param list media:  Media metadata, as a list of media objects
        :param list polls:  Poll metadata, as a list of poll objects
        :param list places:  Place metadata, as a list of place objects
        :param list referenced_tweets:  Tweets referenced in the tweet, as a
        list of tweet objects. These will be enriched in turn.
        :param dict missing_objects: Dictionary with data on missing objects
                from the API by type.
        :return dict:  Enriched tweet object
        """
        # Copy the tweet so that updating this tweet has no effect on others
        tweet = copy.deepcopy(tweet)
        # first create temporary mappings so we can easily find the relevant
        # object later
        users_by_id = {user["id"]: user for user in users}
        users_by_name = {user["username"]: user for user in users}
        media_by_key = {item["media_key"]: item for item in media}
        polls_by_id = {poll["id"]: poll for poll in polls}
        places_by_id = {place["id"]: place for place in places}
        tweets_by_id = {ref["id"]: ref.copy() for ref in referenced_tweets}

        # add tweet author metadata
        tweet["author_user"] = users_by_id.get(tweet["author_id"])

        # add place to geo metadata
        # referenced_tweets also contain place_id, but these places may not included in the place objects
        if 'place_id' in tweet.get('geo', {}) and tweet.get("geo").get("place_id") in places_by_id:
            tweet["geo"]["place"] = places_by_id.get(tweet.get("geo").get("place_id"))
        elif 'place_id' in tweet.get('geo', {}) and tweet.get("geo").get("place_id") in missing_objects.get('place', {}):
            tweet["geo"]["place"] = missing_objects.get('place', {}).get(tweet.get("geo").get("place_id"), {})

        # add user metadata for mentioned users
        for index, mention in enumerate(tweet.get("entities", {}).get("mentions", [])):
            if mention["username"] in users_by_name:
                tweet["entities"]["mentions"][index] = {**tweet["entities"]["mentions"][index], **users_by_name.get(mention["username"])}
            # missing users can be stored by either user ID or Username in Twitter API's error data; we check both
            elif mention["username"] in missing_objects.get('user', {}):
                tweet["entities"]["mentions"][index] = {**tweet["entities"]["mentions"][index], **{'error': missing_objects['user'][mention["username"]]}}
            elif mention["id"] in missing_objects.get('user', {}):
                tweet["entities"]["mentions"][index] = {**tweet["entities"]["mentions"][index], **{'error': missing_objects['user'][mention["id"]]}}


        # add poll metadata
        for index, poll_id in enumerate(tweet.get("attachments", {}).get("poll_ids", [])):
            if poll_id in polls_by_id:
                tweet["attachments"]["poll_ids"][index] = polls_by_id[poll_id]
            elif poll_id in missing_objects.get('poll', {}):
                tweet["attachments"]["poll_ids"][index] = {'poll_id': poll_id, 'error': missing_objects['poll'][poll_id]}

        # add media metadata - seems to be just the media type, the media URL
        # etc is stored in the 'entities' attribute instead
        for index, media_key in enumerate(tweet.get("attachments", {}).get("media_keys", [])):
            if media_key in media_by_key:
                tweet["attachments"]["media_keys"][index] = media_by_key[media_key]
            elif media_key in missing_objects.get('media', {}):
                tweet["attachments"]["media_keys"][index] = {'media_key': media_key, 'error': missing_objects['media'][media_key]}

        # replied-to user metadata
        if "in_reply_to_user_id" in tweet:
            if tweet["in_reply_to_user_id"] in users_by_id:
                tweet["in_reply_to_user"] = users_by_id[tweet["in_reply_to_user_id"]]
            elif tweet["in_reply_to_user_id"] in missing_objects.get('user', {}):
                tweet["in_reply_to_user"] = {'in_reply_to_user_id': tweet["in_reply_to_user_id"], 'error': missing_objects['user'][tweet["in_reply_to_user_id"]]}

        # enrich referenced tweets. Even though there should be no recursion -
        # since tweets cannot be edited - we do not recursively enrich
        # referenced tweets (should we?)
        for index, reference in enumerate(tweet.get("referenced_tweets", [])):
            if reference["id"] in tweets_by_id:
                tweet["referenced_tweets"][index] = {**reference, **self.enrich_tweet(tweets_by_id[reference["id"]], users, media, polls, places, [], missing_objects)}
            elif reference["id"] in missing_objects.get('tweet', {}):
                tweet["referenced_tweets"][index] = {**reference, **{'error': missing_objects['tweet'][reference["id"]]}}

        return tweet

    def fourcatTweetGet(self, queryword, single_date):
        amount=5000
        tweets=0
        md = single_date.strftime("%Y-%m-%d")
        md2 = (single_date + timedelta(days=1)).strftime("%Y-%m-%d")
        mindat = datetime.strptime(md, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")
        maxdat = datetime.strptime(md2, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")
        #print(mindat, maxdat, queryword)
        headers= {"Authorization": "Bearer {}".format(self.bearer_token)}

        tweet_fields = (
        "attachments", "author_id", "context_annotations", "conversation_id", "created_at", "entities", "geo", "id",
        "in_reply_to_user_id", "lang", "public_metrics", "possibly_sensitive", "referenced_tweets", "reply_settings",
        "source", "text", "withheld")
        user_fields = (
        "created_at", "description", "entities", "id", "location", "name", "pinned_tweet_id", "profile_image_url",
        "protected", "public_metrics", "url", "username", "verified", "withheld")
        place_fields = ("contained_within", "country", "country_code", "full_name", "geo", "id", "name", "place_type")
        poll_fields = ("duration_minutes", "end_datetime", "id", "options", "voting_status")
        expansions = (
        "attachments.poll_ids", "attachments.media_keys", "author_id", "entities.mentions.username", "geo.place_id",
        "in_reply_to_user_id", "referenced_tweets.id", "referenced_tweets.id.author_id")
        media_fields = (
        "duration_ms", "height", "media_key", "non_public_metrics", "organic_metrics", "preview_image_url",
        "promoted_metrics", "public_metrics", "type", "url", "width")
        qq= queryword
        if ' ' in queryword:
            qq = '"' + queryword + '"'
        if self.kwlanmap[queryword] == 'ru':
            qq += " (place_country:RU OR lang:ru) -is:retweet"
        elif self.kwlanmap[queryword] == 'uk':
            qq += " (place_country:UA OR lang:uk) -is:retweet"
        elif self.kwlanmap[queryword] == None:
            pass
        else:
            print(queryword)
            sys.exit("Bad keyword lan!")
        qqq=str(qq)
        for mz in ("lang:uk OR place_country:UA", "lang:ru", "lang:ru AND place_country:RU"):
            qqq += ' '+mz + ' -is:retweet'

            o_params = {'query': qqq,
                    'start_time': mindat,
                    'end_time': maxdat,
                "tweet.fields": ','.join(tweet_fields),#self.tweetfields,#self.get_wanted_fields(),
                'user.fields': ','.join(user_fields),#self.userfields,
                'media.fields': ','.join(media_fields),#self.mediafields,
                'place.fields': ','.join(place_fields),#self.placefields,
                'expansions': ','.join(expansions),#self.expansions,
                #'sort_order': 'relevancy',
                'max_results': 100,


            }
            params = o_params
            while True:
                # there is a limit of one request per second, so stay on the safe side of this
                while self.previous_request == int(time.time()):
                    time.sleep(0.1)
                time.sleep(0.05)
                self.previous_request = int(time.time())

                # now send the request, allowing for at least 5 retries if the connection seems unstable
                retries = 5
                api_response = None
                while retries > 0:
                    try:
                        api_response = requests.request("GET", self.url, headers=headers, params=params)
                        #api_response = requests.get(endpoint, headers=auth, params=params, timeout=30)
                        break
                    except (ConnectionError, requests.exceptions.RequestException) as e:
                        retries -= 1
                        wait_time = (5 - retries) * 10
                        print("Got %s, waiting %i seconds before retrying" % (str(e), wait_time))
                        time.sleep(wait_time)

                # rate limited - the limit at time of writing is 300 reqs per 15
                # minutes
                # usually you don't hit this when requesting batches of 500 at
                # 1/second, but this is also returned when the user reaches the
                # monthly tweet cap, albeit with different content in that case
                if api_response.status_code == 429:
                    try:
                        structured_response = api_response.json()
                        if structured_response.get("title") == "UsageCapExceeded":
                            print("Hit the monthly tweet cap. You cannot capture more tweets "
                                                       "until your API quota resets. Dataset completed with tweets "
                                                       "collected so far.")
                            return
                    except (json.JSONDecodeError, ValueError):
                        print("Hit Twitter rate limit, but could not figure out why. Halting "
                                                   "tweet collection.")
                        return

                    resume_at = convert_to_int(api_response.headers["x-rate-limit-reset"]) + 1
                    resume_at_str = datetime.datetime.fromtimestamp(int(resume_at)).strftime("%c")
                    self.dataset.update_status("Hit Twitter rate limit - waiting until %s to continue." % resume_at_str)
                    while time.time() <= resume_at:
                        if self.interrupted:
                            raise ProcessorInterruptedException("Interrupted while waiting for rate limit to reset")
                        time.sleep(0.5)
                    continue

                # API keys that are valid but don't have access or haven't been
                # activated properly get a 403
                elif api_response.status_code == 403:
                    try:
                        structured_response = api_response.json()
                        print("'Forbidden' error from the Twitter API. Could not connect to Twitter API "
                                                   "with this API key. %s" % structured_response.get("detail", ""))
                    except (json.JSONDecodeError, ValueError):
                        print("'Forbidden' error from the Twitter API. Your key may not have access to "
                                                   "the full-archive search endpoint.")
                    finally:
                        return

                # sometimes twitter says '503 service unavailable' for unclear
                # reasons - in that case just wait a while and try again
                elif api_response.status_code in (502, 503, 504):
                    resume_at = time.time() + 60
                    resume_at_str = datetime.datetime.fromtimestamp(int(resume_at)).strftime("%c")
                    print("Twitter unavailable (status %i) - waiting until %s to continue." % (
                    api_response.status_code, resume_at_str))
                    while time.time() <= resume_at:
                        time.sleep(0.5)
                    continue

                # this usually means the query is too long or otherwise contains
                # a syntax error
                elif api_response.status_code == 400:
                    msg = "Response %i from the Twitter API; " % api_response.status_code
                    try:
                        api_response = api_response.json()
                        msg += api_response.get("title", "")
                        if "detail" in api_response:
                            msg += ": " + api_response.get("detail", "")
                    except (json.JSONDecodeError, TypeError):
                        msg += "Some of your parameters (e.g. date range) may be invalid."
                    print(msg)
                    return

                # invalid API key
                elif api_response.status_code == 401:
                    print("Invalid API key - could not connect to Twitter API")
                    return

                # haven't seen one yet, but they probably exist
                elif api_response.status_code != 200:
                    print("Unexpected HTTP status %i. Halting tweet collection." % api_response.status_code)
                    self.log.warning("Twitter API v2 responded with status code %i. Response body: %s" % (
                    api_response.status_code, api_response.text))
                    return

                elif not api_response:
                    print("Could not connect to Twitter. Cancelling.")
                    return

                api_response = api_response.json()
                api_response['locus'] = mz 

                # The API response contains tweets (of course) and 'includes',
                # objects that can be referenced in tweets. Later we will splice
                # this data into the tweets themselves to make them easier to
                # process. So extract them first...
                included_users = api_response.get("includes", {}).get("users", {})
                included_media = api_response.get("includes", {}).get("media", {})
                included_polls = api_response.get("includes", {}).get("polls", {})
                included_tweets = api_response.get("includes", {}).get("tweets", {})
                included_places = api_response.get("includes", {}).get("places", {})

                # Collect missing objects from Twitter API response by type
                missing_objects = {}
                for missing_object in api_response.get("errors", {}):
                    parameter_type = missing_object.get('resource_type', 'unknown')
                    if parameter_type in missing_objects:
                        missing_objects[parameter_type][missing_object.get('resource_id')] = missing_object
                    else:
                        missing_objects[parameter_type] = {missing_object.get('resource_id'): missing_object}
                num_missing_objects = sum([len(v) for v in missing_objects.values()])

                # Record any missing objects in log
                if num_missing_objects > 0:
                    # Log amount
                    print('- -- - - Missing objects collected: ' + ', '.join(['%s: %s' % (k, len(v)) for k, v in missing_objects.items()]))
                if num_missing_objects > 50:
                    # Large amount of missing objects; possible error with Twitter API
                    self.flawless = False
                    print('- - - -- %i missing objects received following tweet number %i. Possible issue with Twitter API.' % (num_missing_objects, tweets))
                    print('- - - - -Missing objects collected: ' + ', '.join(['%s: %s' % (k, len(v)) for k, v in missing_objects.items()]))

                # Warn if new missing object is recorded (for developers to handle)
                expected_error_types = ['user', 'media', 'poll', 'tweet', 'place']
                if any(key not in expected_error_types for key in missing_objects.keys()):
                    print("Twitter API v2 returned unknown error types: %s" % str([key for key in missing_objects.keys() if key not in expected_error_types]))

                # Loop through and collect tweets
                for tweet in api_response.get("data", []):
                    if 0 < amount <= tweets:
                        break

                    # splice referenced data back in
                    # we use copy.deepcopy here because else we run into a
                    # pass-by-reference quagmire
                    tweet = self.enrich_tweet(tweet, included_users, included_media, included_polls, included_places, copy.deepcopy(included_tweets), missing_objects)

                    tweets += 1
                    if tweets % 500 == 0:
                        print("Received %i tweets from the Twitter API" % tweets)

                    yield tweet

                # paginate
                if (amount <= 0 or tweets < amount) and api_response.get("meta") and "next_token" in api_response["meta"]:
                    params["next_token"] = api_response["meta"]["next_token"]
                else:
                    break
    def checkKnown(self):
        filtert = {
        'created_at': {
            '$gte': datetime.strptime(str(self.start_date), '%Y-%m-%d'),
            '$lte': datetime.strptime(str(self.end_date), '%Y-%m-%d')
            }
                }
        filtert = {}#'sourcefile': {'$exists': False}}
        print(filtert)
        tot = self.collection.count_documents(filtert)
        print(tot, 'docs found')
        ct=0
        for post in self.collection.find(filtert):
            ct+=1
            if ct % 100000 == True:
                print('cn-tw', ct/1000, tot/1000)
            self.known_links[post['link']] = 1
        tot = self.arcollection.count_documents(filtert)
        print(tot, 'archive docs found')
        ct=0
        for post in self.arcollection.find(filtert):
            ct+=1
            if ct % 100000 == True:
                print('cn-tw', ct/1000, tot/1000)
            self.known_links[post['link']] = 1

    def getId(self, row):
        try:
            return row['id_str']
        except KeyError:
            return row['id']

    def get_wanted_fields(self):
        flds = ['attachments','author_id','context_annotations','conversation_id','created_at','entities','geo','id','in_reply_to_user_id','lang','non_public_metrics','organic_metrics','possibly_sensitive','promoted_metrics','public_metrics','referenced_tweets','source','text','withheld']
        wants=[]
        for f in flds:
            if not f in ['promoted_metrics', 'non_public_metrics', 'organic_metrics', 'non_public_metrics']:
                wants.append(f)
        wants = ",".join(wants)
        print(wants)
        sys.exit()
        return wants

    def getData(self, queryword, single_date):
        md = single_date.strftime("%Y-%m-%d")
        if False:
            md = single_date.strftime("%Y-%m-%d")
            md2 = (single_date + timedelta(days=1)).strftime("%Y-%m-%d")
            mindat = datetime.strptime(md, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")
            maxdat = datetime.strptime(md2, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")
            print(mindat, maxdat, queryword)
            #params = {'query': '"'+ queryword +'"', 'start_time': mindat, 'end_time': maxdat}
            headers = {"Authorization": "Bearer {}".format(self.bearer_token)}
            qq= queryword
            if ' ' in queryword:
                qq = '"' + queryword + '"'
            if self.kwlanmap[queryword] == 'ru':
                qq += " (place_country:RU OR lang:ru) -is:retweet"
            elif self.kwlanmap[queryword] == 'uk':
                qq += " (place_country:UA OR lang:uk) -is:retweet"
            elif self.kwlanmap[queryword] == None:
                pass
            else:
                print(queryword)
                sys.exit("Bad keyword lan!")

            o_params = {'query': qq,
                    'start_time': mindat,
                    'end_time': maxdat,
                "tweet.fields": self.tweetfields,#self.get_wanted_fields(),
                'user.fields': self.userfields,
                'media.fields': self.mediafields,
                'place.fields': self.placefields,
                'expansions': self.expansions,
                #'sort_order': 'relevancy',
                #'max_results': 100,


            }
            next_token = True
            page=0
            gath=[]
            params = o_params
            hastoken=True
            #this needs to be hourly granularity or smth
            while hastoken:#not next_token is None:
                #print('NT', next_token)
                hastoken=False
                print('Loop', page)
                if len(gath) >= 5000:
                    print("Oveer 5k")
                    break
                #print(params)
                json_response = self.connect_to_endpoint(self.url, headers, params)
                print(json_response['meta'])
                if not 'data' in json_response.keys():
                    print(json_response)
                    print('??? (this probably went OK, if no results')
                    # sys.exit()
                else:
                    for z in json_response['data']:
                        gath.append(z)
                
                try:
                    next_token = json_response['meta']['next_token']
                    params = o_params
                    params['next_token'] = next_token
                    print('NT', params['next_token'])
                    if not next_token is None:
                        hastoken=True
                except KeyError as er:
                    print("ER", er)
                    next_token = None
                page +=1
                print('Hastoken', hastoken)
                time.sleep(2)
                #print('P%s'%page, queryword)
                #print('NT2', next_token)
            print(len(gath), 'results')
        ct=0
        for o in self.fourcatTweetGet(queryword, single_date):
        #for o in gath:
            ct+=1
            link = self.getLink(o)
            try:
                self.known_links[link]
            except KeyError:
                o['keyword'] = queryword
                o['querydate'] = str(datetime.strptime(md, "%Y-%m-%d").date())
                o['link'] = link
                self.collection.insert_one(o)
                #because a post may reoccur in different queries
                self.known_links[link]=1

        time.sleep(3)
        return ct

    def getLink(self, row):
        return 'https://twitter.com/a/status/%s'% self.getId(row)
    def connect_to_endpoint(self, url, headers, params):
        gotten=False
        sleepytime = 3
        while not gotten:
            response = requests.request("GET", self.url, headers=headers, params=params)
            if response.status_code == 429:
                print("Rate limited, waiting %s" % sleepytime)
                time.sleep(sleepytime)
                sleepytime+=3
            elif response.status_code == 200:
                gotten = True
            elif response.status_code != 200:
                raise Exception(
                    "Request returned an error: {} {}".format(
                        response.status_code, response.text
                    )
                )
                sys.exit("SOMETHING WRONG")
        return response.json()

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def get_queries_to_get(self):
        wants=[]
        known ={}
        for post in self.qcoll.find():
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
    def doSearch(self):
        #untested
        for q in self.queries:
            if os.path.isfile('results/%s_tweets.p' % q):
                sys.exit("FILE EXISTS")
            gath=[]
            url = "https://api.twitter.com/2/tweets/search/all?max_results=500&query=%s&expansions=%s&user.fields=%s" % (quote_plus(q), expansions, user_fields)
            # url += "&tweet.fields=attachments,author_id,id,text,entities&user.fields=id,name,profile_image_url,url,username&expansions=referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,in_reply_to_user_id,attachments.media_keys&media.fields=preview_image_url,type,url"
            print('>>>>>>>>>>>>>>>', url)
            headers = {"Authorization": "Bearer {}".format(bearer_token)}
            o_params = {"tweet.fields": self.get_wanted_fields()}
            # o_params={}
            params = o_params
            next_token = True
            page=1
            while next_token:
                json_response = self.connect_to_endpoint(url, headers, params)
                if not 'data' in json_response.keys():
                    print(json_response)
                    print('??? (this probably went OK, if no results')
                    # sys.exit()
                else:
                    for z in json_response['data']:
                        gath.append(z)
                next_token = False
                try:
                    next_token = json_response['meta']['next_token']
                    params = o_params
                    params['next_token'] = next_token
                except KeyError:
                    pass
                page +=1
                print(page, q)
                #pickle.dump(gath, open('results/%s_tweets.p' % q,'wb'))
                if randint(0,10) == 5:
                    time.sleep(randint(1,4))
        print('Got', len(gath), 'items')
    def startDownload(self):
        self.get_queries_to_get()
        print(len(self.toget), 'queries left')
        for i, query in enumerate(self.toget):
            queryword, dt = query
            #if not queryword == 'вторжения' or not dt == datetime.strptime('2022-04-04','%Y-%m-%d').date():
            #    continue
            print('Get', '%s/%s' % (i, len(self.toget)), query)
            no_got = self.getData(queryword, dt)
            self.qcoll.insert_one({'q': queryword, 'dt': str(dt)})
            print("Gotten", queryword, str(dt), no_got)
            #if no_got > 100:
            #    sys.exit('!!')
        print("Done getting data (for now)!")


if __name__ == '__main__':

    vk = Tweeter()
    q2g = vk.startDownload()
