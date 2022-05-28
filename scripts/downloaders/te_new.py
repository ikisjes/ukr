import os, codecs, csv, time
import pymongo
from telethon import TelegramClient
from datetime import date, datetime, timezone,timedelta
import asyncio, traceback
from telethon.errors.rpcerrorlist import UsernameInvalidError, TimeoutError, ChannelPrivateError, BadRequestError, FloodWaitError

class FourcatException(Exception):
	pass

class QueueException(FourcatException):
	"""
	General Queue Exception - only children are to be used
	"""
	pass

class ProcessorException(FourcatException):
	"""
	Raise if processor throws an exception
	"""
	pass


class JobClaimedException(QueueException):
	"""
	Raise if job is claimed, but is already marked as such
	"""
	pass


class JobAlreadyExistsException(QueueException):
	"""
	Raise if a job is created, but a job with the same type/remote_id combination already exists
	"""
	pass


class JobNotFoundException(QueueException):
	"""
	Raise if trying to instantiate a job with an ID that is not valid
	"""
	pass

class QueryParametersException(FourcatException):
	"""
	Raise if a dataset query has invalid parameters
	"""
	pass
class WorkerInterruptedException(FourcatException):
	"""
	Raise when killing a worker before it's done with its job
	"""
	pass
class ProcessorInterruptedException(WorkerInterruptedException):
	"""
	Raise when killing a processor before it's done with its job
	"""
	pass

class TelSearch:
    min_date=None
    max_date=None
    eventloop=None
    flawless=True
    interrupted=False
    qcoll=None
    collection=None
    arcollection=None
    start_date=date(2022,3,29)
    end_date=datetime.now().date() - timedelta(days=1)
    end_if_rate_limited=600
    max_retries=3
    max_workers=1
    toget=[]
    known_links={}
    def __init__(self):
        client = pymongo.MongoClient("mongodb://localhost:27017")

        db = client['original']
        self.collection = db['te']
        dbold = client['archive']
        self.arcollection = db['te']
        qdb = client['queries']
        self.qcoll = qdb['tequeries']
        parameters = {
            'api_id': ,
            'api_hash': '',
            'api_phone': '',
            'queries': []
        }
        q=[]
        ct=0
        known={}
        with codecs.open('/var/scripts/downloaders/telechans.txt','r', encoding='utf-8') as f:
            for chan in f:
                chan=chan.strip()
                try:
                    known[chan]
                    continue
                except KeyError:
                    known[chan]=1
                q.append(chan)
                if len(q) == 24:
                    parameters['queries'].append(q)
                    q=[]
                if len(q) > 1:
                    break
        if not q == []:
            parameters['queries'].append(q)
        self.parameters = parameters
        self.checkKnown()

    def checkKnown(self):
        print("Checking known - in case something crashed")
        for post in self.collection.find():
            self.known_links[post['link']] = 1
        for post in self.arcollection.find():
            self.known_links[post['link']] = 1
        print("Done!")
    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def get_queries_to_get(self):
        wants=[]
        known ={}
        for single_date in self.daterange(self.start_date, self.end_date):
            for queries in self.parameters['queries']:
                for queryword in queries:
                    known[queryword] = datetime(2022,3,29,0,0,0)
        for post in self.qcoll.find():
            #known["%s_%s"% (post['q'], post['dt'])] = 1
            dt = datetime.strptime(post['dt'], "%Y-%m-%d")#.date()
            try:
                if dt > known[post['q']]:
                    known[post['q']] = dt
            except KeyError:
                known[post['q']] = dt


        self.toget=[]
        now = datetime.now()# - timedelta(days=1)
        now = now.replace(hour=0, minute=0, second=0)
        for k, v in known.items():
            self.toget.append((k, v.replace(hour=0,minute=0,second=0), now))

    @staticmethod
    def serialize_obj(input_obj):
        """
        Serialize an object as a dictionary
        Telethon message objects are not serializable by themselves, but most
        relevant attributes are simply struct classes. This function replaces
        those that are not with placeholders and then returns a dictionary that
        can be serialized as JSON.
        :param obj:  Object to serialize
        :return:  Serialized object
        """
        scalars = (int, str, float, list, tuple, set, bool)

        if type(input_obj) in scalars or input_obj is None:
            return input_obj

        if type(input_obj) is not dict:
            obj = input_obj.__dict__
        else:
            obj = input_obj.copy()

        mapped_obj = {}
        for item, value in obj.items():
            if type(value) is datetime:
                mapped_obj[item] = value.timestamp()
            elif type(value).__module__ in ("telethon.tl.types", "telethon.tl.custom.forward"):
                mapped_obj[item] = TelSearch.serialize_obj(value)
                if type(obj[item]) is not dict:
                    mapped_obj[item]["_type"] = type(value).__name__
            elif type(value) is list:
                mapped_obj[item] = [TelSearch.serialize_obj(item) for item in value]
            elif type(value).__module__[0:8] == "telethon":
                # some type of internal telethon struct
                continue
            elif type(value) is bytes:
                mapped_obj[item] = value.hex()
            elif type(value) not in scalars and value is not None:
                # type we can't make sense of here
                continue
            elif type(value) is dict:
                for key, vvalue in value:
                    mapped_obj[item][key] = TelSearch.serialize_obj(vvalue)
            else:
                mapped_obj[item] = value

        return mapped_obj
    
    def anytoget(self):

        for qquery in self.toget:#get_queries_to_get():
            query, min_date, max_date = qquery
            if min_date.date() == max_date.date():
                print("Skipping, min=max")
                continue
            print("MOAR", query)
            return True
        return False

    @staticmethod
    def cancel_start():
        """
        Replace interactive phone number input in Telethon
        By default, if Telethon cannot use the given session file to
        authenticate, it will interactively prompt the user for a phone
        number on the command line. That is not useful here, so instead
        raise a RuntimeError. This will be caught below and the user will
        be told they need to re-authenticate via 4CAT.
        """
        raise RuntimeError("Connection cancelled")        
    async def execute_queries(self):
        """
        Get messages for queries
        This is basically what would be done in get_items(), except due to
        Telethon's architecture this needs to be called in an async method,
        which is this one.
        :return list:  Collected messages
        """
        # session file has been created earlier, and we can re-use it here in
        # order to avoid having to re-enter the security code
        query = self.parameters

        # session_id = TelSearch.create_session_id(query["api_phone"], query["api_id"], query["api_hash"])
        # self.dataset.log('Telegram session id: %s' % session_id)
        # session_path = Path(config.PATH_ROOT).joinpath(config.PATH_SESSIONS, session_id + ".session")

        client = None

        try:
            # client = TelegramClient(str(session_path), int(query.get("api_id")), query.get("api_hash"),
                                    # loop=self.eventloop)
            client = TelegramClient('anon', self.parameters['api_id'], self.parameters['api_hash'],
                                    loop=self.eventloop)
            await client.start(phone=TelSearch.cancel_start)
        except RuntimeError:
            # session is no longer useable, delete file so user will be asked
            # for security code again. The RuntimeError is raised by
            # `cancel_start()`
            print(
                "Session is not authenticated: login security code may have expired. You need to re-enter the security code.")

            if client and hasattr(client, "disconnect"):
                await client.disconnect()

            if session_path.exists():
                session_path.unlink()

            return []
        except Exception as e:
            # not sure what exception specifically is triggered here, but it
            # always means the connection failed
            print("Telegram: %s\n%s" % (str(e), traceback.format_exc()))
            print("Error connecting to the Telegram API with provided credentials.")
            if client and hasattr(client, "disconnect"):
                await client.disconnect()
            return []

        #for qquery in self.toget:#get_queries_to_get():
        #    print('>>>', qquery)
        #    print('>>>', qquery)
        for qquery in self.toget:#get_queries_to_get():
            print(qquery)
            query, min_date, max_date = qquery
            if min_date.date() == max_date.date():
                print("Skipping, min=max")
                continue
            max_items = 50000

            max_date = max_date.date()
            # min_date can remain an integer
            min_date = min_date.timestamp()

            posts = []
            toinsert=[]
            queries = [query]
            try:
                async for post in self.gather_posts(client, queries, max_items, min_date, max_date):
                    post['link'] = self.getLink(post)
                    try:
                        self.known_links[post['link']]
                        continue
                    except KeyError:
                        self.known_links[post['link']]=1
                    except Exception as wer:
                        print(wer)
                        sys.exit('!!!!')
                    toinsert.append(post)
                    if len(toinsert)> 100:
                        try:
                            self.collection.insert_many(toinsert)
                        except pymongo.errors.BulkWriteError as e:
                            panic = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
                            if len(panic) > 0:
                                raise
                    posts.append(post)
                print(queries, 'done')
                if len(toinsert)> 0:
                    try:
                        self.collection.insert_many(toinsert)
                    except pymongo.errors.BulkWriteError as e:
                        panic = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
                        if len(panic) > 0:
                            raise
                self.qcoll.insert_one({'q': query, 'dt': str(max_date)})
                print("Collected posts for ", query)
                return posts
            except ProcessorInterruptedException as e:
                print("Interrupt!")
                raise e
            except Exception as e:
                # catch-all so we can disconnect properly
                # ...should we?
                print("Error scraping posts from Telegram")
                print("Telegram scraping error: %s" % traceback.format_exc())
                return []
            finally:
                print("Reached finally")
                await client.disconnect()
        print("wtf happened")
        
    
    async def gather_posts(self, client, queries, max_items, min_date, max_date):
        """
        Gather messages for each entity for which messages are requested
        :param TelegramClient client:  Telegram Client
        :param list queries:  List of entities to query (as string)
        :param int max_items:  Messages to scrape per entity
        :param int min_date:  Datetime date to get posts after
        :param int max_date:  Datetime date to get posts before
        :return list:  List of messages, each message a dictionary.
        """
        resolve_refs = self.parameters.get("resolve-entities")
        min_date_fmt=datetime.utcfromtimestamp(min_date)#.strftime('%Y-%m-%d %H:%M:%S')
        #print(qqqq)
        #sys.exit('??')

        # Adding flag to stop; using for rate limits
        no_additional_queries = False
        max_items=50000

        # Collect queries
        for query in queries:
            delay = 10
            retries = 0

            if no_additional_queries:
                # Note that we are note completing this query
                print("Rate-limited by Telegram; not executing query %s" % query)
                continue

            while True:
                print("Fetching messages for entity '%s'" % query)
                i = 0
                try:
                    entity_posts = 0
                    async for message in client.iter_messages(entity=query, offset_date=max_date):
                        entity_posts += 1
                        i += 1
                        if self.interrupted:
                            raise ProcessorInterruptedException(
                                "Interrupted while fetching message data from the Telegram API")

                        if entity_posts % 100 == 0:
                            print(
                                "Retrieved %i posts for entity '%s' (%i total)" % (entity_posts, query, i))

                        if message.action is not None:
                            # e.g. someone joins the channel - not an actual message
                            continue

                        # todo: possibly enrich object with e.g. the name of
                        # the channel a message was forwarded from (but that
                        # needs extra API requests...)
                        serialized_message = TelSearch.serialize_obj(message)
                        if resolve_refs:
                            serialized_message = await self.resolve_groups(client, serialized_message)
                        dt = datetime.combine(min_date_fmt, datetime.min.time())
                        timestamp = (dt - datetime(1970, 1, 1)) / timedelta(seconds=1)

                        # Stop if we're below the min date
                        if min_date and serialized_message.get("date") < timestamp:
                            break

                        yield serialized_message

                        if entity_posts >= max_items:
                            break

                except ChannelPrivateError:
                    print("Entity %s is private, skipping" % query)
                    self.flawless = False

                except (UsernameInvalidError,):
                    print("Could not scrape entity '%s', does not seem to exist, skipping" % query)
                    self.flawless = False

                except FloodWaitError as e:
                    print("Rate-limited by Telegram: %s; waiting" % str(e))
                    if e.seconds < self.end_if_rate_limited:
                        time.sleep(e.seconds)
                        continue
                    else:
                        self.flawless = False
                        no_additional_queries = True
                        print("Telegram wait grown large than %i minutes, ending" % int(e.seconds/60))
                        break

                except BadRequestError as e:
                    print("Error '%s' while collecting entity %s, skipping" % (e.__class__.__name__, query))
                    self.flawless = False

                except ValueError as e:
                    print("Error '%s' while collecting entity %s, skipping" % (str(e), query))
                    self.flawless = False

                except ChannelPrivateError as e:
                    print(
                        "QUERY '%s' unable to complete due to error %s. Skipping." % (
                        query, str(e)))
                    break

                except TimeoutError:
                    if retries < 3:
                        print(
                            "Tried to fetch messages for entity '%s' but timed out %i times. Skipping." % (
                            query, retries))
                        self.flawless = False
                        break

                    print(
                        "Got a timeout from Telegram while fetching messages for entity '%s'. Trying again in %i seconds." % (
                        query, delay))
                    time.sleep(delay)
                    delay *= 2
                    continue

                break


    def get_items(self):
        """
        Execute a query; get messages for given parameters
        Basically a wrapper around execute_queries() to call it with asyncio.
        :param dict query:  Query parameters, as part of the DataSet object
        :return list:  Posts, sorted by thread and post ID, in ascending order
        """
        # if "api_phone" not in query or "api_hash" not in query or "api_id" not in query:
            # self.dataset.update_status("Could not create dataset since the Telegram API Hash and ID are missing. Try "
                                       # "creating it again from scratch.")
            # return None

        results = asyncio.run(self.execute_queries())

        if not self.flawless:
            print("Dataset completed, but some requested entities were unavailable (they may have"
                                       "been private). View the log file for details.")

        return results
    def getLink(self, row):
        #print(row.keys())
        #for x in row.keys():
        #    try:
        #        print([x, row[x].keys()])
        #    except Exception:
        #        print([x, row[x]])
        try:
            return 'https://t.me/%s/%s' % (row['search_entity'], row['id'])
        except KeyError:
            try:
                return 'https://t.me/%s/%s' % (row['author_username'], row['id'])
            except KeyError:
                return 'https://t.me/%s/%s' % (row['_chat']['username'], row['id'])

if __name__ == '__main__':
    os.chdir('/var/scripts/downloaders')
    t=TelSearch()
    t.min_date = datetime.strptime("2022-03-29", "%Y-%m-%d").date()
    t.max_date = datetime.strptime("2022-04-02", "%Y-%m-%d").date()
    t.get_queries_to_get()
    while t.anytoget():
        #posts = t.get_items()
        known = {}
        for q in t.collection.find():
            known[q['link']]=1
        toinsert=[]
        for p in t.get_items():
            # print(p)
            dt = datetime.utcfromtimestamp(float(p['date'])).strftime('%Y-%m-%d %H:%M:%S')
            link = t.getLink(p)
            try:
                known[link]
                continue
            except KeyError:
                known[link] = 1
                p['link'] = link
                toinsert.append(p)
            if len(toinsert)> 100:
                try:
                    t.collection.insert_many(toinsert)
                except pymongo.errors.BulkWriteError as e:
                    panic = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))

                    if len(panic) > 0:
                        raise
                toinsert=[]
            # dt = datetime.fromtimestamp(float(p['date']) / 1e3)
            #print(dt, p['date'],   " ".join(p['message'].split()))
        if len(toinsert)> 0:
            try:
                t.collection.insert_many(toinsert)
            except pymongo.errors.BulkWriteError as e:
                #to ignore duplicatee key things
                panic = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))

                if len(panic) > 0:
                    raise
        t.get_queries_to_get()
