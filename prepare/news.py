import pymongo as pm
import datetime as dt
import os
import fileinput
import shutil
import subprocess
import gensim as gs
import pandas as pd
import numpy as np

class Document:
    def __init__(self, parse):
        self.parse = parse

class Bow:
    def __init__(self, docList, termQuant, userDictSet = None):
        self.userDictSet = userDictSet if userDictSet is not None else set()
        self.termQuant = termQuant
        self.docList = docList
        self.parseList = []
        for doc in docList:
            self.parseList.append(doc.parse)
        self.wordId = gs.corpora.Dictionary(self.parseList)
        for word, id in self.wordId.token2id.items():
            self.wordId.id2token[id] = word
        corpus = [self.wordId.doc2bow(parse) for parse in self.parseList]

        tfIdf = gs.models.TfidfModel(corpus)
        for c in range(len(corpus)):
            cor = corpus[c]
            ti = tfIdf[cor]
            tiDict = {} # use for sort
            for i in range(len(ti)):
                ti[i] = [self.wordId.id2token[ti[i][0]], ti[i][1]]
                tiDict[ti[i][0]] = ti[i][1]
            tiSort = sorted(tiDict.items(), key=lambda d:d[1], reverse=True)
            self.docList[c].tiSort = tiSort
            self.docList[c].tiDict = tiDict

        self.termToId = {}
        self.idToTerm = {}
        maxTermId = 0
        for d in range(len(self.docList)):
            doc = self.docList[d]
            termSet = set()
            for ts in range(len(doc.tiSort)):
                word = doc.tiSort[ts][0]
                if (ts <= int(len(doc.tiSort) * self.termQuant)) or word in self.userDictSet:
                    termSet.add(word)
            termList = []
            for word in doc.parse:
                if word in termSet:
                    termList.append(word)
                    if word not in self.termToId.keys():
                        self.termToId[word] = maxTermId
                        self.idToTerm[maxTermId] = word
                        maxTermId += 1
            doc.termList = termList

    def GetId(self, word):
        return self.termToId[word]

    def GetWord(self, id):
        return self.idToTerm[id]

    def GetVocabSize(self):
        return len(self.termToId.keys())

    def SaveIdTerm(self, path):
        fileIdToTerm = open(os.path.join(path, 'idToTerm.txt'), 'w')
        for id, term in self.idToTerm.items():
            fileIdToTerm.write(str(id) + ',' + str(term) + os.linesep)
        fileIdToTerm.flush()
        fileIdToTerm.close()

        fileTermToId = open(os.path.join(path, 'termToId.txt'), 'w')
        for term, id in self.termToId.items():
            fileTermToId.write(str(term) + ',' + str(id) + os.linesep)
        fileTermToId.flush()
        fileTermToId.close()


def ReadFileLineAsWord(path):
    wordSet = set()
    for word in fileinput.input(path):
        if word[-1] == os.linesep:
            word = word[0 : len(word) - 1]
        if len(word) >= 1:
            wordSet.add(word)
    return wordSet


def UpdateNews(endDate, connStr, days,
               dividHour=8, dividMinute=0, lastDividHour=15, lastDividMinute=0):
    mc = pm.MongoClient(connStr)
    db = mc.text
    col = db['section']
    corpusPath = os.path.join('.', 'data', 'news', 'corpus')
    eachDayPath = os.path.join('.', 'data', 'news', 'each_day')
    eachOvernightPath = os.path.join('.', 'data', 'news', 'each_overnight')
    endDate = dt.datetime(endDate.date().year, endDate.date().month, endDate.date().day)
    beginDate = endDate - dt.timedelta(days=days)

    # clear old files
    if os.path.exists(corpusPath):
        shutil.rmtree(corpusPath)
    os.mkdir(corpusPath)
    if os.path.exists(eachDayPath):
        shutil.rmtree(eachDayPath)
    os.mkdir(eachDayPath)
    if os.path.exists(eachOvernightPath):
        shutil.rmtree(eachOvernightPath)
    os.mkdir(eachOvernightPath)

    # read new news
    calendarPath = os.path.join('.', 'data', 'calendar')
    dateList = []
    for dateStr in fileinput.input(calendarPath):
        if(len(dateStr) > 1):
            if dateStr[-1] == os.linesep:
                dateStr = dateStr[0: len(dateStr) - 1]
            dateList.append(dt.datetime.strptime(dateStr, '%Y-%m-%d'))
    dateList.reverse()

    for dateSeq in range(len(dateList) - 1):
        today = dateList[dateSeq]
        yesterday = dateList[dateSeq + 1]
        print(today)
        if today < beginDate:
            break
        dayPath = os.path.join('.', 'data', 'news', 'each_day', today.strftime('%Y-%m-%d'))
        if os.path.exists(dayPath):
            shutil.rmtree(dayPath)
        os.mkdir(dayPath)
        overnightPath = os.path.join('.', 'data', 'news', 'each_overnight', today.strftime('%Y-%m-%d'))
        if os.path.exists(overnightPath):
            shutil.rmtree(overnightPath)
        os.mkdir(overnightPath)

        dividTime = yesterday + dt.timedelta(hours=lastDividHour, minutes=lastDividMinute)
        sectionList = col.find({'time': {'$gte': yesterday + dt.timedelta(hours=dividHour, minutes=dividMinute),
                                         '$lt': today + dt.timedelta(hours=dividHour, minutes=dividMinute)}})
        for section in sectionList:
            if section['masterId'] != '':
                continue
            fileName = section['_id'].replace('/', '_')
            fileName = fileName[max(0, len(fileName) - 100) : min(len(fileName), max(0, len(fileName) - 100) + 100)]
            fileName = section['time'].strftime('%Y%m%d%H%M%S') + '_' + fileName
            # write to corpus folder
            corpusFile = open(os.path.join(corpusPath, fileName), 'w')
            corpusFile.write(section['secTitle'] + os.linesep)
            corpusFile.write(section['content'])
            corpusFile.close()
            # linked into eachday folder
            os.symlink(os.path.join(os.getcwd(), 'data', 'news', 'corpus', fileName), 
                       os.path.join(dayPath, fileName))
            # linked into eachovernight folder
            if section['time'] > dividTime:
                os.symlink(os.path.join(os.getcwd(), 'data', 'news', 'corpus', fileName), 
                           os.path.join(overnightPath, fileName))


def Segment(mergeThreshold, minCount, modelPath, lastDividHour=15, lastDividMinute=0):
    paraStr = '-mf ./data/news/merge_forbid.txt -ud ./data/news/user_dict -print 0 '
    for threshold in mergeThreshold:
        paraStr += '-mt ' + str(threshold) + ' '
    paraStr += '-mc ' + str(minCount) + ' -mp ' + modelPath
    
    newsEachDayPath = os.path.join('.', 'data', 'news', 'each_day')
    newsSegEachDayPath = os.path.join('.', 'data', 'news_seg', 'each_day')
    allDateDirectorys= os.listdir(newsEachDayPath)
    returnInfoDict = {}
    for dateStr in allDateDirectorys:
        newsDayDateDirectory = os.path.join(newsEachDayPath, dateStr) + os.path.sep
        newsSegDayDateDirectory = os.path.join(newsSegEachDayPath, dateStr) + os.path.sep
        if not os.path.isfile(newsDayDateDirectory) and not os.path.exists(newsSegDayDateDirectory):
            # segment
            os.mkdir(newsSegDayDateDirectory)
            command = './segment ' + paraStr + ' -cp ' + newsDayDateDirectory + \
                      ' -tp ' + newsSegDayDateDirectory
            s = subprocess.Popen(str(command), stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
            stdErrorInfo, stdOutInfo = s.communicate()
            returnCode = s.returncode
            returnInfoDict[dateStr] = \
                {'returnCode': returnCode, 'stdErrorInfo': stdErrorInfo, 'stdOutInfo': stdOutInfo}
        else:
            returnInfoDict[dateStr] = 'ignored'

    return returnInfoDict


            # # linked into overnight folder
            # newsSegEachOvernightPath = os.path.join('.', 'data', 'news_seg', 'each_overnight')
            # newsSegOvernightDateDirectory = os.path.join(newsSegEachOvernightPath, dateStr) + os.path.sep
            # if os.path.exists(newsSegOvernightDateDirectory):
            #     shutil.rmtree(newsSegOvernightDateDirectory)
            # os.mkdir(newsSegOvernightDateDirectory)
            # segDayDirectory = os.path.join(newsSegDayDateDirectory, 'seg')
            # segMergeDayDirectory = os.path.join(newsSegDayDateDirectory, 'seg_merge')
            # segOvernightDirectory = os.path.join(newsSegOvernightDateDirectory, 'seg')
            # segMergeOvernightDirectory = os.path.join(newsSegOvernightDateDirectory, 'seg_merge')
            # os.mkdir(segOvernightDirectory)
            # os.mkdir(segMergeOvernightDirectory)

            # allSegFiles = os.listdir(segDayDirectory)
            # for fileName in allSegFiles:
            #     fileTime = dt.datetime.strptime(fileName[0 : 14], '%Y%m%d%H%M%S')


def WordVec(beginDateStr = '00010101', endDateStr = '99991231', termQuant = 0.5,
            vecSize = 100, windowSize = 10, minCount = 5, simTermCount = 5):
    '''
    get similar words for each theme word
    :param beginDateStr: begin date string for wordvec model
    :param endDateStr: end date string for wordvec model
    :param termQuant: remove the words which's tf-idf quant is lower than this parameter
    :param vecSize: the size of wordvec
    :param windowSize: the size of context of wordvec
    :param minCount: the words which's frequency of occurrence are less than this parameter will be ignored
    :param simTermCount: how many similar words will be related to a theme word
    :return: a dict, which's key is a theme word, and value is a similar words set
    '''

    segFilePath = os.path.join('.', 'data', 'news_seg', 'each_day')
    segType = 'seg_merge'
    seperator = ' '

    allDateStr = os.listdir(segFilePath)
    docList = []
    for dateStr in allDateStr:
        if dateStr > endDateStr or dateStr < beginDateStr:
            continue
        datePath = os.path.join(segFilePath, dateStr)
        if os.path.isfile(datePath):
            continue 
        datePath = os.path.join(datePath, segType)
        allFileName = os.listdir(datePath)
        for fileName in allFileName:
            if fileName[0] == '.':
                continue 
            filePath = os.path.join(datePath, fileName)
            wordList = []
            for line in fileinput.input(filePath):
                wordList.extend(line.split(seperator))
            docList.append(Document(wordList))
    userDictPath = os.path.join('.', 'data', 'news', 'user_dict')
    bow = Bow(docList, termQuant, userDictSet = ReadFileLineAsWord(userDictPath))
    segList = []
    for doc in bow.docList:
        segList.append(doc.termList)
    wordVecModel = gs.models.Word2Vec(segList, size=vecSize, window=windowSize, min_count=minCount)

    themeWordSet = ReadFileLineAsWord(os.path.join('.', 'data', 'news', 'theme_word'))
    vocabSet = set(wordVecModel.wv.index2word)
    themeWordRelationDict = {}
    for themeWord in themeWordSet:
        relateWordsSet = set()
        relateWordsSet.add(themeWord)
        if themeWord in vocabSet:
            relateWordsList = wordVecModel.wv.most_similar_cosmul(positive=[themeWord], topn=simTermCount)
            for relateWords in relateWordsList:
                relateWordsSet.add(relateWords[0])
        themeWordRelationDict[themeWord] = relateWordsSet

    # wordVecDict = {}
    # for w in range(len(wordVecModel.wv.index2word)):
    #     word = wordVecModel.wv.index2word[w]
    #     vec = wordVecModel.wv.syn0norm[w]
    #     wordVecDict[word] = vec

    return themeWordRelationDict #, wordVecDict


def GetThemeSentences(dateStr, wordVecBeginDateStr = None, simTermCount = 5):
    '''
    find sentences which are related to each theme word
    :param dateStr: end date string
    :param wordVecBeginDateStr: begin date string for wordvec model, It will be specified as dateStr if it's None,
    :param simTermCount: the number of similar words those are related to a theme word
    :return: a dict, of which the key is a theme word, and the value is a related sentences list
    '''
    wordSep = ' '
    senSepSet = {'。', '！', '？', '；', '\n'}
    datePath = os.path.join('.', 'data', 'news_seg', 'each_day', dateStr, 'seg_merge')
    if not (os.path.exists(datePath) and os.path.isdir(datePath)):
        raise Exception('date path ' + datePath + ' does not exist!')

    if wordVecBeginDateStr is None:
        wordVecBeginDateStr = dateStr
    themeWordRelationDict = WordVec(wordVecBeginDateStr, dateStr, simTermCount=simTermCount)
    relationReverseDict = {}
    for themeWord, relateWordsSet in themeWordRelationDict.items():
        for relateWord in relateWordsSet:
            if relateWord not in relationReverseDict.keys():
                relationReverseDict[relateWord] = set()
            relationReverseDict[relateWord].add(themeWord)

    relatedSentenceListDict = {}
    for themeWord in themeWordRelationDict.keys():
        relatedSentenceListDict[themeWord] = []
    
    for fileName in os.listdir(datePath):
        fileFullPath = os.path.join(datePath, fileName)
        if os.path.isdir(fileFullPath):
            continue 

        senWordList = []
        relatedThemeWordSet = set()
        for line in fileinput.input(fileFullPath):
            lineParse = line.split(wordSep)
            for word in lineParse:
                senWordList.append(word)
                if word in relationReverseDict.keys():
                    for themeWord in relationReverseDict[word]:
                        relatedThemeWordSet.add(themeWord)
                if word in senSepSet and len(senWordList) > 0: # a sentence
                    for relatedThemeWord in relatedThemeWordSet:
                        relatedSentenceListDict[relatedThemeWord].append(senWordList)
                    senWordList = []
                    relatedThemeWordSet = set()

    return relatedSentenceListDict

def GetVolumnGrowth(beginDate, endDate, aveDays = 14):
    '''
    get trade volumn growth for all stocks in the special day
    :param beginDate: begin date
    :param endDate: end date
    :return: a dataframe, the rows is date, and the columns is stock name, and element is volumn growth
    '''
    file = open(os.path.join('.', 'data', 'name_to_symbol'))
    nameToSymbol = {}
    lines = file.readlines()
    for line in lines:
        if len(line) >= 3:
            line = line.replace('\r\n', '')
            line = line.replace('\n', '')
            strSplit = line.split(',')
            nameToSymbol[strSplit[0]] = strSplit[1]
    file.close()

    dateList = []
    i = 0
    while(beginDate + dt.timedelta(days=i) <= endDate):
        dateList.append(beginDate + dt.timedelta(days=i))
        i += 1

    volDf = pd.DataFrame(index=dateList, columns=list(nameToSymbol.keys()), dtype=float)
    for name, symbol in nameToSymbol.items():
        filePath = os.path.join('.', 'data', 'price', symbol)
        if not os.path.exists(filePath):
            continue

        for line in fileinput.input(filePath):
            if len(line) <= 1:
                continue
            lineSeg = line.split(',')
            if len(lineSeg) != 6:
                print(' a invalid line in ' + filePath + ': ' + line)
                continue
            date = dt.datetime.strptime(lineSeg[0], '%Y-%m-%d')
            if beginDate <= date <= endDate:
                volumn = float(lineSeg[4])
                volDf.loc[date][name] = volumn

    volGrowthDf = pd.DataFrame(index=dateList, columns=list(nameToSymbol.keys()), dtype=float)
    for i in range(volDf.shape[0]):
        for j in range(volDf.shape[1]):
            if i <= aveDays:
                volGrowthDf.iloc[i, j] = np.nan
            else:
                aveVol = np.nanmean(volDf.iloc[i - aveDays : i, j].values)
                volGrowthDf.iloc[i, j] = (volDf.iloc[i, j] / aveVol - 1) if aveVol != 0 else np.nan

    return volGrowthDf