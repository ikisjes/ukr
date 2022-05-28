from query import Query

if __name__ == '__main__':
    platforms = ['vk','te','tw','fb']
    q=Query()
    q.get_basestats()
    #platforms=['tw']
    period = ['2022-04-28', '2022-05-11']
    if True:
        #1. Top 25 most engaged posts per week
        #platforms = ['te']
        #platforms = ['fb']
        #platforms = ['vk']
        #platforms = ['tw']
        for platform in platforms:
            if platform == 'te':
                q.compile_query(before_date= period[1], after_date = period[0], per=['lid','week','topic' ,'language'], fields_to_sum=['engagement'],fields_to_count=[], splitper=['week','topic','language'], only_platform=platform,extrafields=['username','text','date','link','keywords','telegramdata'],only_lans=['ru','uk'], top=100)
            else:
                #q.compile_query(after_date = '2022-04-01', per=['lid','week','topic' ,'language'], fields_to_sum=['engagement'],fields_to_count=[], splitper=['week','topic','language'], only_platform=platform,extrafields=['username','text','date','link','keywords'],only_lans=['ru','uk'], top=100)
                q.compile_query(before_date = period[1], after_date = period[0], per=['lid','week','topic' ,'language'], fields_to_sum=['engagement'],fields_to_count=[], splitper=['week','topic','language'], only_platform=platform,extrafields=['username','text','date','link','keywords'],only_lans=['ru','uk'], top=100)
    if True:
        #2. Top 5 most mentioned keywords
        q.topKeywordsOverTime(without_general=True)
    if True:
        for x in ['Санкции', 'Буча']:
            q.sentences_that_mention(x, language='ru', only_te_aff_rus=True)
    if True:
        #5. Mentions of debunkers
        q.who_mentions_debunkers()
    if True:
        for x in [r'Правда заключается в том', r'на самом деле', r'в реальности', r'Фейк']:
            q.bigrams_after_phrase_te_ruaff( phrase=x, platform=None, language='ru')
    if True:
        #3. Posts over time per stance, language platform
        q.compile_query(per=['date','topic','language','platform'], fields_to_count=['lid'])
    


    if True:
        #4. Rankflows
        q.rankflow_db()



    #q.get_basestats()
    #q.who_mentions_debunkers()
    #q.who_mentions_chan('inside')
    #q.who_mentions_chan('outside')
    #for w in ['Правда заключается в том', 'на самом деле', 'в реальности']:
	#    q.bigrams_after_phrase(w)

    #for w in ['ложь', 'обман', 'Фейк', 'измена', 'ересь']:
	#    q.bigrams_after(w)

    #q.telegramChannelsForTopics()
    #q.top_vk_ru_links()
    #q.co_url_disinfo('Allegations of disinformation')


    #gets killed
    #q.makeNetwork()
