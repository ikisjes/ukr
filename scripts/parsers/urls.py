import sys, psycopg2
import requests
from tld import get_tld
import tld
from validator import URLValidator, ValidationError

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class urlResolver:
    url=None
    resolve=None
    tld=None
    cur=None
    browser=None
    
    def getBrowser(self):
        if self.browser is None:
            try:
                webdriver
            except Exception:
                from selenium import webdriver
                from webdriver_manager.chrome import ChromeDriverManager
                from webdriver_manager.utils import ChromeType
                from selenium.webdriver.chrome.options import Options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument('ignore-certificate-errors')
            chrome_options.add_argument('log-level=3')
            self.browser = webdriver.Chrome(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install(), options=chrome_options)

                
            #options = webdriver.ChromeOptions()
            #options.add_argument('headless')
            #options.add_argument('ignore-certificate-errors')
            #options.add_argument('log-level=3')
            #self.browser = webdriver.Chrome(options=options)
            self.browser.set_page_load_timeout(60)

    # try:
        # validators
    # except Exception:
        # import validators
    def __init__(self):
        self.connect()
        self.isvalid = URLValidator()
    def connect(self):
        if not self.cur:
            self.conn=psycopg2.connect(database="ukraine", user="dmanagy", password="bcyasbCBStc6@@dcdc", host='127.0.0.1',port=5432)
            self.conn.set_client_encoding('UTF8')
            self.cur = self.conn.cursor()
    
    def doresolve(self, url=None):
        self.url = url
        #make sure it exists
        self.cur.execute("select url from urls where url like %(u)s", {'u': url})
        row=self.cur.fetchone()
        if not row:
            self.cur.execute("insert into urls (url) values (%(u)s)", {'u':url})
            self.conn.commit()
        try:
            self.isvalid(url)
        except ValidationError as er: 
            self.cur.execute("update urls set resolve ='ERROR' where url = %(u)s and resolve is null", {
                'u': self.url
            })
            self.conn.commit()
            return [None,None]
        # try:
            # validators.url(url)
        # except Exception:
            # self.resolve = 'ERROR'
            # self.tld = 'ERROR'
            # self.cur.execute("update urls set resolve ='ERROR' where url = %(u)s", {
                # 'u': self.url
            # })
            # self.conn.commit()
            # print("Invalid", self.url)
            # return
        #do we have it yet?
        self.cur.execute("select url, resolve, tld from urls where url = %(u)s", {'u': url})
        row=self.cur.fetchone()
        updatetld=False
        if row:
            self.resolve = row[1]
            self.tld = row[2]
            if not row[2]:
                updatetld=True
        else:
            updatetld=True
        if not self.resolve:
            try:
                self.resolve = self.quickresolver()
            except Exception as wer:
                print('quick',wer)
                try:
                    self.slowresolve()
                except Exception as rer:
                    print('RER',rer)
            if self.resolve:
                self.cur.execute("update urls set resolve = %(r)s where url = %(u)s", {
                    'u': self.url,
                    'r': self.resolve,
                })
                self.conn.commit()
                # print('>', self.tld)
            #except Exception as er:
            #    print('er',er)
        if self.resolve and not self.tld:
            try:
                self.findTld()
            except tld.exceptions.TldBadUrl:
                self.tld ='ERROR'
            updatetld=True
        if updatetld and self.tld:
            self.cur.execute("update urls set tld = %(t)s where url = %(u)s", {
                'u': self.url,
                't': self.tld
                })
        return [self.resolve , self.tld]


    def quickresolver(self):
        r = requests.head(self.url, allow_redirects=True, timeout=5, verify=False)
        return r.url

    def slowresolve(self):
        self.getBrowser()
        self.browser.get(self.url)
        self.resolve = self.browser.current_url
        print("selenium url", self.resolve)
        
        

    def findTld(self):
        t = get_tld(self.resolve, as_object=True)
        t = "%s.%s" % (t.domain, t.tld)
        self.tld = t
