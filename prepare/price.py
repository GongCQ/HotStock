import cx_Oracle as co
import os
import datetime as dt

def UpdatePrice(begin, end, connStr):
    pricePath = os.path.join('.', 'data', 'price')
    configPath = os.path.join(pricePath, 'config')
    try:
        configFile = open(configPath)
        begin = dt.datetime.strptime(configFile.readline(), '%Y-%m-%d')
    except Exception as e:
        for file in os.listdir(pricePath):
            targetFile = os.path.join(pricePath, file)
            if os.path.isfile(targetFile):
                os.remove(targetFile)
        configFile = open(configPath, 'w')
        configFile.write(begin.strftime('%Y-%m-%d'))
        configFile.close()

    sql = "SELECT I.STK_CODE, I.STK_SHORT_NAME, M.TRADE_DATE, M.OPEN_PRICE_RE, M.CLOSE_PRICE_RE, " \
          "       M.RISE_DROP_RANGE_RE, M.TRADE_VOL, M.TRADE_AMUT " \
          "FROM UPCENTER.STK_BASIC_PRICE_MID M JOIN UPCENTER.STK_BASIC_INFO I " \
          "         ON M.ISVALID = 1 AND I.ISVALID = 1 AND I.STK_TYPE_PAR = 1 AND" \
          "            M.STK_UNI_CODE = I.STK_UNI_CODE " \
          "WHERE M.TRADE_VOL > 0 AND " \
          "      M.TRADE_DATE > TO_DATE('{BEGIN_DATE}', 'YYYY-MM-DD') AND " \
          "      M.TRADE_DATE <= TO_DATE('{END_DATE}', 'YYYY-MM-DD') " \
          "ORDER BY M.TRADE_DATE "
    conn = co.connect(connStr)
    cursor = conn.cursor()
    cursor.execute(
        sql.replace('{BEGIN_DATE}', begin.strftime('%Y-%m-%d')).replace('{END_DATE}', end.strftime('%Y-%m-%d')))
    recordList = cursor.fetchall()
    stockFileDict = {}
    nameToSymbol = {}
    for stockFile in os.listdir(pricePath):
        stockFileDict[stockFile] = open(os.path.join(pricePath, stockFile), 'a')
    for record in recordList:
        symbol = record[0]
        name = record[1]
        nameToSymbol[name] = symbol
        if symbol not in stockFileDict.keys():
            stockFileDict[symbol] = open(os.path.join(pricePath, symbol), 'a')
        stockFile = stockFileDict[symbol]
        stockFile.write(record[2].strftime('%Y-%m-%d') + ',' + str(record[3]) + ',' + str(record[4]) + ',' +
                             str(record[5]) + ',' + str(record[6]) + ',' + str(record[7]) + os.linesep)

    nameToSymbolFilePath = os.path.join('.', 'data', 'name_to_symbol')
    if os.path.exists(nameToSymbolFilePath):
        file = open(nameToSymbolFilePath)
        lines = file.readlines()
        for line in lines:
            if len(line) >= 3:
                line = line.replace('\r\n', '')
                line = line.replace('\n', '')
                strSplit = line.split(',')
                if strSplit[0] not in nameToSymbol.keys():
                    nameToSymbol[strSplit[0]] = strSplit[1]
        file.close()
    file = open(nameToSymbolFilePath, 'w')
    for name, symbol in nameToSymbol.items():
        file.write(name + ',' + symbol + os.linesep)
    file.flush()
    file.close()

    os.remove(configPath)
    configFile = open(configPath, 'w')
    configFile.write(end.strftime('%Y-%m-%d'))
    configFile.close()