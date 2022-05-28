import csv, codecs
from datetime import timedelta

def get_keywords(remove_asterisks=True, w_language=False):
    kw=[]
    kwl=[]
    with codecs.open('/var/scripts/keywords.csv','r', encoding='utf-8') as f:
        r=csv.DictReader(f, delimiter='\t')
        for row in r:
            if row['Keyword (please watch out for stemming *)'].strip():
                if not row['Keyword (please watch out for stemming *)'] == 'мир':
                    kw.append( row['Keyword (please watch out for stemming *)'].strip().replace('*',''))
                    kwl.append( (row['Language'], row['Keyword (please watch out for stemming *)'].strip().replace('*','')))
    if w_language:
        return kwl
    return kw


def read_keywords():
    kw={}
    with codecs.open('/var/scripts/keywords.csv','r', encoding='utf-8') as f:
        r=csv.DictReader(f, delimiter='\t')
        for row in r:
            if row['Keyword (please watch out for stemming *)'].strip():
                if not row['Keyword (please watch out for stemming *)'] == 'мир':
                    try:
                        kw[row['Theme']]
                    except KeyError:
                        kw[row['Theme']]={}
                    try:
                        kw[row['Theme']][row['Language']]
                    except KeyError:
                        kw[row['Theme']][row['Language']]=[]
                    kw[row['Theme']][row['Language']].append(row['Keyword (please watch out for stemming *)'].strip().replace('#',''))
    return kw

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


