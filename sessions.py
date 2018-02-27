import os
import ib
import utils
import sqlite3
import datetime
import checkers
import contracts
import kristal_functions
from ibapi.wrapper import iswrapper


class IBCalculateKristalNavSession(ib.IB):
    def __init__(self, host, port, clientId):
        super(IBCalculateKristalNavSession, self).__init__()
        self.connect(host, port, clientId)
        self.gui = kristal_functions.KristalRiskNavigator()
        self.kristalNavHandler = kristal_functions.KristalNavHandler()
        self.tickPriceHandler = kristal_functions.KristalTickPriceHandler()
        self.kristalClientHandler = kristal_functions.KristalClientHandler()

    @staticmethod
    def _iterateOverModelPortfolio(modelPortfolios):
        for kristalName, assets in modelPortfolios.items():

            # Determine correct kristal name as kristal names in model portfolio tend to be cut off midway
            # due to Excel's limitation of 31 characters max.
            correctKristalName = checkers.check_kristal_name(kristalName)

            # Determine plausible kristal currencies in a list
            plausibleKristalCurrencies = [
                kristal_functions.determineKristalCurrency(assetCurrency) for assetCurrency in assets['currency'].values
            ]

            # Ultimately there can ony be one kristal currency, so based on all the possibilities,
            # I will be able to pinpoint exactly the currency.
            if plausibleKristalCurrencies.count(kristal_functions.IN_JURISDICTION_CURRENCY) > 0:
                kristalCurrency = kristal_functions.IN_JURISDICTION_CURRENCY
            elif plausibleKristalCurrencies.count(kristal_functions.SG_JURISDICTION_CURRENCY) < \
                    len(plausibleKristalCurrencies):
                kristalCurrency = kristal_functions.HK_JURISDICTION_CURRENCY
            else:
                kristalCurrency = kristal_functions.SG_JURISDICTION_CURRENCY

            # After determining the kristal currency, proceed to check the details of each row of data iterated
            # More often than not, the details in this file is not fully compatible with IB contracts, and thus
            # require cleaning up. The module which we use for cleaning is mainly checkers.py.
            for asset_name, assetDetails in assets.iterrows():

                # Parsing out the details into variables for easier manipulation
                symbol, exchange, currency, secType, qty = \
                    assetDetails.loc['symbol'], assetDetails.loc['exchange'], assetDetails.loc['currency'], \
                    assetDetails.loc['asset_type'], assetDetails.loc['quantity']

                if secType == 'CASH':
                    symbol, exchange = kristalCurrency, 'IDEALPRO'  # forex checker built into contract making function
                    fxContract = None
                elif secType == 'STK':
                    symbol, exchange, currency = checkers.check_equity_details(symbol, exchange, currency)
                    fxContract = contracts.Contract('CASH', currency, 'IDEALPRO', kristalCurrency)
                elif secType == 'OPT':
                    symbol, exchange, currency = checkers.check_option_details(symbol, exchange, currency)
                    fxContract = contracts.Contract('CASH', currency, 'IDEALPRO', kristalCurrency)
                elif secType == 'FUT':
                    symbol, exchange, currency = checkers.check_futures_details(symbol, exchange, currency)
                    fxContract = contracts.Contract('CASH', currency, 'IDEALPRO', kristalCurrency)
                elif secType == 'BONDS':
                    symbol, exchange, currency = checkers.check_bond_details(symbol, exchange, currency)
                    fxContract = contracts.Contract('CASH', currency, 'IDEALPRO', kristalCurrency)
                elif secType == 'MFD':
                    break
                else:
                    raise NotImplementedError

                assetContract = contracts.Contract(secType, symbol, exchange, currency)
                yield qty, correctKristalName, kristalCurrency, assetContract, fxContract

    def calculateNav(self, modelPortfolios, subscriptions):
        """
        Iterate over the model portfolio file to make tick data requests to Interactive Brokers
        :param modelPortfolios: (Excel file) Excel file with each sheet containing asset details on one kristal
        :return: Nothing
        """

        self.kristalClientHandler.store(subscriptions)

        for qty, kristalName, kristalCurrency, assetContract, fxContract in \
                self._iterateOverModelPortfolio(modelPortfolios):

            if fxContract is not None:
                if not self.tickPriceHandler.recordExists(fxContract):
                    reqId = self.getNextId()
                    self.tickPriceHandler.createFxRecord(reqId, fxContract)
                    self.startMktData(reqId, fxContract)
                else:
                    prevReqId = self.tickPriceHandler.getPrevReqIdForFx(fxContract)
                    self.tickPriceHandler.createFxRecord(prevReqId, fxContract)

            if not self.tickPriceHandler.recordExists(assetContract):
                reqId = self.getNextId()
                self.tickPriceHandler.createRecord(reqId, qty, assetContract, kristalName, kristalCurrency)
                self.startMktData(reqId, assetContract)
            else:
                prevReqId = self.tickPriceHandler.getPrevReqIdForFx(assetContract)
                self.tickPriceHandler.createRecord(prevReqId, qty, assetContract, kristalName, kristalCurrency)
                self.startMktData(prevReqId, assetContract)

        self.run()

    def startMktData(self, reqId, contract, genericTickList='', snapshot=False,
                     regulatorySnapshot=False, mktDataOptions=None):
        """
        This was meant to override thr getMktData function but the different number of arguments dictate that it
        might be better to create a new function but replicate the contents.
        :param reqId: (int)
        :param contract: (contracts.Contract)
        :param genericTickList: (str) See
        :param snapshot: (Boolean)
        :param regulatorySnapshot: (Boolean)
        :param mktDataOptions: (List)
        :return: Nothing
        """
        self.logger.info('#Request %d Started: Tick data for %s' % (reqId, contract.getSymbol()))
        self.reqMktData(reqId, contract, genericTickList, snapshot, regulatorySnapshot, mktDataOptions)

    @iswrapper
    def tickPrice(self, reqId, tickType, price, attrib):
        super().tickPrice(reqId, tickType, price, attrib)
        self.logger.debug('#Request %d In Progress: Tick Price (%d) Received' % (reqId, tickType))
        self.tickPriceHandler.refreshRecord(reqId, tickType, price, attrib)
        self.kristalNavHandler.refresh(self.tickPriceHandler)
        self.kristalClientHandler.refresh(self.kristalNavHandler)
        self.gui.plotHandlers(self.kristalNavHandler, self.kristalClientHandler)


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
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (ibContract.getSymbol(), )
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
