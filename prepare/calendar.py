import cx_Oracle as co
import os

def UpdateCalendar(begin, end, connStr):
    sql = "SELECT C.END_DATE, C.IS_TRADE_DATE " \
          "FROM UPCENTER.PUB_EXCH_CALE C " \
          "WHERE C.IS_TRADE_DATE = 1 AND C.SEC_MAR_PAR = 1 AND " \
          "      C.END_DATE BETWEEN TO_DATE('{BEGIN_DATE}', 'YYYY-MM-DD') AND TO_DATE('{END_DATE}', 'YYYY-MM-DD') " \
          "ORDER BY END_DATE"
    conn = co.connect(connStr)
    cursor = conn.cursor()
    cursor.execute(sql.replace('{BEGIN_DATE}', begin.strftime('%Y-%m-%d')).replace('{END_DATE}', end.strftime('%Y-%m-%d')))
    tradeDateList = cursor.fetchall()

    calendarFilePath = os.path.join('.', 'data', 'calendar')
    if os.path.exists(calendarFilePath) and os.path.isfile(calendarFilePath):
        os.remove(calendarFilePath)
    calendarFile = open(calendarFilePath, 'w')
    for record in tradeDateList:
        calendarFile.write(record[0].strftime('%Y-%m-%d') + os.linesep)
    calendarFile.flush()
    calendarFile.close()