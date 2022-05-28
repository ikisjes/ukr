from query import Query
import sys

if __name__ == '__main__':

    platforms = ['vk','te','tw','fb']
    q=Query()
    q.top100posts()
    sys.exit()
    period=['2022-04-28', '2022-05-11']
    for platform in platforms:
        if platform == 'te':
            q.compile_query(before_date=period[1], after_date=period[0], per=['lid','topic','language'], fields_to_sum=['engagement'], only_platform=platform, extrafields=['username','text','date','link','keywords','telegramdata'], only_lans=['uk','ru'], top=100)
        else:
            q.compile_query(before_date=period[1], after_date=period[0], per=['lid','topic','language'], fields_to_sum=['engagement'], only_platform=platform, extrafields=['username','text','date','link','keywords'], only_lans=['uk','ru'], top=100)

    sys.exit()
    q.compile_query(per=['date','topic','language','platform'], fields_to_count=['lid'])
    sys.exit()
    q.rankflow_db(languages=['ru'], platforms=['tw'])
    sys.exit()
    q.compile_query(per=['date','topic','language','platform'], fields_to_count=['lid'])
    sys.exit()
    q.compile_query(per=['date','platform','language','text','engagement'], qquery='Гутерриш'.lower())
    sys.exit()
    #RUNNING
    #q.compile_query(per=['date','topic','platform','language'], fields_to_count=['lid'])


    #RAN
    #q.topKeywordsOverTime()
    #RUNNING
    for pf in platforms:
        q.compile_query(per=['week','platform','language','topic'], fields_to_sum=['engagement'], fields_to_count=['lid'], splitper=['topic','language'], only_lans=['ru','uk'], only_platform=pf, after_date='2022-01-01')
    
    #RUNNING
    #q.who_mentions_debunkers()
    sys.exit()

    #q.compile_query(per=['lid','week','topic','language'], only_lans=['uk'], fields_to_sum=['engagement'], fields_to_count=[], splitper=['week','topic','language'], only_platform='tw', extrafields=['username','text','date','link','keywords','telegramdata'], after_date='2020-04-18', top=100)
    #q.compile_query(per=['lid','week','topic','language'], only_lans=['ru'], fields_to_sum=['engagement'], fields_to_count=[], splitper=['week','topic','language'], only_platform='tw', extrafields=['username','text','date','link','keywords','telegramdata'], after_date='2020-04-18', top=100)
    
    q.compile_query(per=['lid','topic','language'], fields_to_sum=['engagement'], fields_to_count=[], splitper=['topic','language'], only_lans=['ru','uk'], only_platform='tw', extrafields=['username','text','date','link','keywords'], after_date='2022-04-12', before_date='2022-04-27', top=100)
    #q.compile_query(per=['lid','topic','language'], fields_to_sum=['engagement'], fields_to_count=[], splitper=['topic','language'], only_lans=['ru','uk'], only_platform='te', extrafields=['username','text','date','link','keywords','telegramdata'], after_date='2022-04-12', before_date='2022-04-27', top=100)
    sys.exit()

    for w in ['Правда заключается в том','на самом деле','в реальности','Фейк']:
	    q.bigrams_after_phrase_te_ruaff(w)
    sys.exit()
    q.topKeywordsOverTime()

    sys.exit()
    q.compile_query(per=['urls'], fields_to_sum=['engagement'],top=100)
    #sys.exit()
    q.getdict(per=['tlds'], fields_to_count=['lid'])
    #sys.exit()
    q.makeNetwork(between=['username','tlds'],limit=False)
    #sys.exit()
    q.getTopUrls(tlds_instead=True,counts_instead=True)
    #sys.exit()
    q.getTldCount()
    #sys.exit()

    for w in ['Правда заключается в том', 'на самом деле', 'в реальности']:
        # q.bigrams_after_phrase(phrase=w)
        q.bigrams_after_phrase_for_debunk_channels(phrase=w)
    #sys.exit()
    q.compile_query(per=['date'], username='itsdonetsk', only_platform='te', fields_to_sum=['engagement'],fields_to_count=['lid'])
    #sys.exit()
    q.compile_query(per=['date','platform'], fields_to_sum=['engagement'],fields_to_count=['lid'],qquery='#своихнебросаем')
    q.get_basestats()
    q.who_mentions_debunkers()
    q.who_mentions_chan('inside')
    q.who_mentions_chan('outside')
    for w in ['Правда заключается в том', 'на самом деле', 'в реальности']:
	    q.bigrams_after_phrase(w)

    for w in ['ложь', 'обман', 'Фейк', 'измена', 'ересь']:
	    q.bigrams_after(w)

    q.telegramChannelsForTopics()
    q.top_vk_ru_links()
    q.co_url_disinfo('Allegations of disinformation')
    #for platform in platforms:
    #    q.compile_query(per=['lid','week','topic' ,'language'], fields_to_sum=['engagement'],fields_to_count=[], splitper=['week','topic','language'], only_platform=platform,extrafields=['username','text','date','link','keywords'],only_lans=['ru','uk'], top=50)


    #gets killed
    #q.makeNetwork()
