import os
import ib
import utils
import sqlite3
import datetime


class IBHistoricalDataSession(ib.IB):
    def __init__(self, host, port, clientId):
        super(IBHistoricalDataSession, self).__init__()
        self.connect(host, port, clientId)

    def extract(self, ibContracts, durationString, barSizeSetting, whatToShow,
                useRTH, formatDate, keepUpToDate, chartOptions):

        conn = sqlite3.connect(os.path.join(utils.PATH, utils.determineDatabaseToUse(whatToShow)))
        cursor = conn.cursor()

        for ibContract in ibContracts:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (ibContract.symbol, )
            )
            relevantTables = cursor.fetchall()

            if not relevantTables:
                endDatetime = ''
                self.getHeadTimeStampAndHistoricalData(ibContract, endDatetime, durationString,
                                                       barSizeSetting, whatToShow, useRTH,
                                                       formatDate, keepUpToDate, chartOptions)
            else:
                nameOfTable = relevantTables[0]
                cursor.execute('SELECT date FROM ? DESC LIMIT 1', nameOfTable)
                lastParsedDateStr = cursor.fetchall()[0]

                today = datetime.datetime.now()
                lastParsedDatetime = datetime.datetime.strptime(lastParsedDateStr, utils.IB_DATE_FMT)
                daysDiff = (today - lastParsedDatetime).days
                self.getHistoricalData(ibContract, lastParsedDateStr, str(daysDiff) + ' D', barSizeSetting, whatToShow,
                                       useRTH, formatDate, keepUpToDate, chartOptions)

        conn.close()
        self.run()
