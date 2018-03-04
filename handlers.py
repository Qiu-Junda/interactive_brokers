import etc
import databases
import pandas as pd


class IBHistoricBarHandler:
    def __init__(self):
        self.records = {}

    def createRecord(self, reqId, event):
        self.records[reqId] = event

    def editRecord(self, reqId, **kwargs):
        event = self.records[reqId]
        event.edit(kwargs)
        self.records[reqId] = event

    def getRecord(self, reqId):
        return self.records[reqId]

    def reindexRecord(self, newReqId, oldReqId):
        self.records[newReqId] = self.records.pop(oldReqId)

    def closeRecord(self, reqId):
        event = self.records[reqId]

        bars = event.bars
        whatToShow = event.whatToShow
        ticker = event.contract.symbol

        df = pd.concat(bars, axis=1, join='outer').T.iloc[::-1]

        if whatToShow == 'ADJUSTED_LAST':
            db = databases.HistoricalAdjustedLastDatabase()
        elif whatToShow == 'TRADES':
            db = databases.HistoricalTradesDatabase()
        else:
            raise NotImplementedError

        db.connect()
        db.insertDf(ticker, df)
        db.close()

        event.bars = []
        self.records[reqId] = event


class IBTickHandler:
    def __init__(self):
        self.records = {}
        # self.movingAverageHandler = MovingAverageHandler()

    def createRecord(self, reqId, event):
        self.records[reqId] = event

    def refreshRecord(self, reqId, tickType, *args):
        if tickType == etc.BID_PRICE_TICK_TYPE:
            self.records[reqId]['bid'] = args[0]
        elif tickType == etc.ASK_PRICE_TICK_TYPE:
            self.records[reqId]['ask'] = args[0]
        elif tickType == etc.LAST_PRICE_TICK_TYPE:
            self.records[reqId]['last'] = args[0]
        else:
            if not self.records[reqId]['last']:
                if tickType == etc.LAST_RTH_TRADED_PRICE:
                    self.records[reqId]['last'] = args[0]

                if not self.records[reqId]['last']:
                    if tickType == etc.CLOSE_PRICE_TICK_TYPE:
                        self.records[reqId]['last'] = args[0]

    def closeRecord(self, reqId):
        del self.records[reqId]
