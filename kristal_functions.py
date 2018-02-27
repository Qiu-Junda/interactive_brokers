import math
import utils
import asyncio
import threading
import numpy as np
import pandas as pd
import tkinter as tk


O2O_INV_ACCT_PORT = 9999  # I just put an arbitrary value here, needs to be changed
ASHEESH_ACCT_PORT = 5043
IN_JURISDICTION_CURRENCY = 'INR'
SG_JURISDICTION_CURRENCY = 'SGD'
HK_JURISDICTION_CURRENCY = 'USD'


def determineKristalCurrency(currency):
    if currency == IN_JURISDICTION_CURRENCY:
        return IN_JURISDICTION_CURRENCY
    elif currency != SG_JURISDICTION_CURRENCY:
        return HK_JURISDICTION_CURRENCY
    else:
        return SG_JURISDICTION_CURRENCY


class KristalRiskNavigator:
    def __init__(self):
        pass

    def _plot(self, loop):
       pass

    @staticmethod
    def _nearestSquare(num):
        return math.ceil(np.sqrt(num))

    def plotHandlers(self, kristalNavHandler, kristalClientHandler):
        width = self._nearestSquare(len(kristalNavHandler.navs))



class KristalClientHandler(tk.Tk):
    def __init__(self):
        super(KristalClientHandler, self).__init__()
        self.subscriptions = None

    def store(self, subscriptions):
        self.subscriptions = subscriptions

    def refresh(self, kristalNavHandler):
        pass


class KristalNavHandler(tk.Tk):
    def __init__(self):
        super(KristalNavHandler, self).__init__()
        self.positions = None
        self.notional = None
        self.prices = None
        self.navs = None

    def _convertFx(self, tickPriceHandler, assetCurrency, kristalCurrency):
        for fxIdx, fxRow in tickPriceHandler.fxRecords.iterrows():
            if (
                    fxRow.loc[tickPriceHandler.assetCurrencyCol] == assetCurrency and
                    fxRow.loc[tickPriceHandler.kristalCurrencyCol] == kristalCurrency
            ) or (
                    fxRow.loc[tickPriceHandler.assetCurrencyCol] == kristalCurrency and
                    fxRow.loc[tickPriceHandler.kristalCurrencyCol] == assetCurrency
            ):
                return fxRow.loc[tickPriceHandler.midCol]

    def refresh(self, tickPriceHandler):
        """
        Uses the data inside the tickPriceHandler and from model portfolio to compute kristal nav
        :param tickPriceHandler: (KristalTickPriceHandler)
        :return: A GUI display illustrating the NAVs
        """

        if self.positions is None:
            self.positions = tickPriceHandler.records.pivot(index=tickPriceHandler.kristalCol,
                                                            columns=tickPriceHandler.symbolCol,
                                                            values=tickPriceHandler.qtyCol).fillna(0)

        for assetIdx, assetRow in tickPriceHandler.assetRecords.iterrows():
            assetRow.loc[tickPriceHandler.fxConvertedLastCol] = \
                assetRow.loc[tickPriceHandler.lastCol] * \
                self._convertFx(tickPriceHandler,
                                assetRow.loc[tickPriceHandler.assetCurrencyCol],
                                assetRow.loc[tickPriceHandler.kristalCurrencyCol])

        self.prices = tickPriceHandler.records.pivot(index=tickPriceHandler.kristalCol,
                                                     columns=tickPriceHandler.symbolCol,
                                                     values=tickPriceHandler.fxConvertedLastCol)
        self.notional = self.prices * self.positions
        self.navs = self.notional.sum(axis=1)


class KristalTickPriceHandler:
    """
    This class helps to maintain the latest state of all tick prices required to price the model portfolios.
    """
    def __init__(self):
        self.fxRecords = pd.DataFrame()
        self.assetRecords = pd.DataFrame()
        self.navHandler = KristalNavHandler()

        self.qtyCol = 'qty'
        self.reqIdCol = 'reqId'
        self.symbolCol = 'symbol'
        self.assetCurrencyCol = 'currency'
        self.kristalNameCol = 'kristalName'
        self.kristalCurrencyCol = 'kristalCurrency'
        self.fxConvertedLastCol = 'fxConvertedLast'
        self.bidCol, self.askCol, self.lastCol, self.midCol = 'bid', 'ask', 'last', 'mid'

    def recordExists(self, contract):
        """
        Checks if record exists in self.records by using symbols
        :param contract: (contracts.Contract)
        :return: (Boolean) True or False
        """
        symbol = contract.getSymbol()
        if contract.secType == 'CASH':
            for idx, row in self.fxRecords.iterrows():
                if (row.loc[self.symbolCol] == symbol and row.loc[self.kristalCurrencyCol] == contract.currency) or \
                        (row.loc[self.kristalCurrencyCol] == symbol and row.loc[self.symbolCol] == contract.currency):
                    return True
            return False
        else:
            for idx, row in self.assetRecords.iterrows():
                if row.loc[self.symbolCol] == symbol:
                    return True
            return False

    def createRecord(self, reqId, qty, contract, kristalName, kristalCurrency):
        """
        Creates asset record WITHOUT the reqId.
        :param reqId: (int) Unique identifier to Interactive Brokers' API
        :param qty: (int) Number of units for this contract for given kristal
        :param contract: (contracts.Contract)
        :param kristalName: (str) Name of kristal
        :param kristalCurrency: (str) The currency which the kristal is denominated in
        :return: Nothing
        """
        symbol = contract.getSymbol()
        assetDetails = pd.DataFrame(
            data=[reqId, kristalName, symbol, contract.currency, kristalCurrency, qty, None, None, None, None],
            index=[self.reqIdCol, self.kristalNameCol, self.symbolCol, self.assetCurrencyCol,
                   self.kristalCurrencyCol, self.qtyCol, self.bidCol, self.askCol, self.lastCol,
                   self.fxConvertedLastCol]).T
        self.assetRecords = pd.concat([self.assetRecords, assetDetails], axis=0, join='outer')

    def getPrevReqIdForAssets(self, contract):
        """
        Find the previous reqId used by a existing contract
        :param contract: (contracts.Contract)
        :return: (int) Previous reqId
        """
        symbol = contract.getSymbol()
        prevReqId = self.assetRecords.loc[self.assetRecords[self.symbolCol] == symbol, self.reqIdCol]
        if prevReqId.empty:
            raise ValueError('Cannot find the prevReqId for %s' % symbol)
        return prevReqId.values[0]

    def createFxRecord(self, reqId, contract):
        """
        Creates FX record WITHOUT the reqId
        :param reqId: (int) Unique identifier to Interactive Brokers' API
        :param contract: (contracts.Contract)
        :return: Nothing
        """
        symbol = contract.getSymbol()
        currency = contract.currency
        fxDetails = pd.DataFrame(
            data=[reqId, symbol, currency, None, None, None],
            index=[self.reqIdCol, self.symbolCol, self.assetCurrencyCol, self.bidCol,
                   self.askCol, self.midCol, self.fxConvertedLastCol]
        ).T
        self.fxRecords = pd.concat([self.fxRecords, fxDetails], axis=0, join='outer')

    def getPrevReqIdForFx(self, contract):
        symbol = contract.getSymbol()
        currency = contract.currency
        prevReqId = self.fxRecords.loc[(self.fxRecords[self.symbolCol] == symbol) &
                                       (self.fxRecords[self.assetCurrencyCol] == currency), self.reqIdCol]
        if prevReqId.empty:
            prevReqId = self.fxRecords.loc[(self.fxRecords[self.symbolCol] == currency) &
                                           (self.fxRecords[self.assetCurrencyCol] == symbol), self.reqIdCol]
            if prevReqId.empty:
                raise ValueError('Cannot find the prevReqId for %s' % symbol)
        return prevReqId.values[0]

    def refreshRecord(self, reqId, tickType, price, attrib):
        if reqId in self.fxRecords[self.reqIdCol].values:

            # There are no last prices for fx. So compute mid price from bid ask.
            if tickType == utils.BID_PRICE_TICK_TYPE:
                self.fxRecords.loc[self.fxRecords[self.reqIdCol] == reqId, self.bidCol] = price
            elif tickType == utils.ASK_PRICE_TICK_TYPE:
                self.fxRecords.loc[self.fxRecords[self.reqIdCol] == reqId, self.askCol] = price
            self.fxRecords.loc[self.fxRecords[self.reqIdCol] == reqId, self.midCol] = self._getMidPrice(reqId, 'fx')

        elif reqId in self.assetRecords[self.reqIdCol].values:

            # Tick price types in order of priority. Highest priority from the left:
            # Last Price > Mid Price > Last RTH Price > Close Price > Delayed Last Price
            if tickType == utils.LAST_PRICE_TICK_TYPE:
                self.assetRecords.loc[self.assetRecords[self.reqIdCol] == reqId, self.lastCol] = price
            elif tickType in [utils.BID_PRICE_TICK_TYPE, utils.ASK_PRICE_TICK_TYPE]:
                if tickType == utils.BID_PRICE_TICK_TYPE:
                    self.assetRecords.loc[self.assetRecords[self.reqIdCol] == reqId, self.bidCol] = price
                else:
                    self.assetRecords.loc[self.assetRecords[self.reqIdCol] == reqId, self.askCol] = price
                self.assetRecords.loc[self.assetRecords[self.reqIdCol] == reqId, self.midCol] = \
                    self._getMidPrice(reqId, 'asset')
            elif tickType == utils.LAST_RTH_TRADED_PRICE:
                self.assetRecords.loc[self.assetRecords[self.reqIdCol] == reqId, self.lastCol] = price
            elif tickType == utils.CLOSE_PRICE_TICK_TYPE:
                self.assetRecords.loc[self.assetRecords[self.reqIdCol] == reqId, self.lastCol] = price
            else:
                pass
        else:
            raise NotImplementedError('ReqId %d cannot be found in either FX records or asset records.' % reqId)

    def _getMidPrice(self, reqId, recordType):
        if recordType == 'fx':
            bid = self.fxRecords.loc[self.fxRecords[self.reqIdCol] == reqId, self.bidCol]
            ask = self.fxRecords.loc[self.fxRecords[self.reqIdCol] == reqId, self.askCol]
        else:
            bid = self.assetRecords.loc[self.assetRecords[self.reqIdCol] == reqId, self.bidCol]
            ask = self.assetRecords.loc[self.assetRecords[self.reqIdCol] == reqId, self.askCol]
        if not bid:
            return ask
        elif not ask:
            return bid
        else:
            return (bid + ask) / 2
