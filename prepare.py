import prepare.calendar as calendar
import prepare.price as price
import prepare.news as news
import prepare.user_dict as user_dict
import prepare.wordvec as wordvec
import config.getconfig as getconfig
import os
import datetime as dt
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

paraDict = getconfig.GetPara(os.path.join('.', 'config', 'config'))

# news.UpdateNews(dt.datetime.now(), paraDict['mongoConn'], 7, 8, 0)
# news.Segment([0.6, 0.5], 5, './thulac/models/')
# themeWordRelationDict = news.WordVec(beginDateStr = '2018-03-29', endDateStr = '2018-03-29')
news.GetThemeSentences('2018-03-29')

calendar.UpdateCalendar(dt.datetime.now() - dt.timedelta(days = 365),
                        dt.datetime.now() - dt.timedelta(days = 1), paraDict['oraConn'])
price.UpdatePrice(dt.datetime.now() - dt.timedelta(days = 60),
                  dt.datetime.now() - dt.timedelta(days = 1), paraDict['oraConn'])
news.GetVolumnGrowth(dt.datetime(2018, 1, 30), dt.datetime(2018, 4, 30), 7)
