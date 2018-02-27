import utils
import logging
import datetime
import handlers
from ibapi.client import EClient
from ibapi.utils import iswrapper
from ibapi.wrapper import EWrapper


class IB(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        self.reqId = 0
        self.logger = logging.getLogger('Interactive Brokers')
        self.historicBarHandler = handlers.IBHistoricBarHandler()
        self.tickGenericHandler = handlers.IBTickGenericHandler()
        self.tickPriceHandler = handlers.IBTickPriceHandler()
        self.tickSizeHandler = handlers.IBTickSizeHandler()

    # ==================================  other api functions  ============================================

    def getNextId(self):
        self.reqId += 1
        return self.reqId

    # ==================================  Changing market data type functions  ================================

    def changeMarketDataType(self, marketDataType):
        if marketDataType == 1:
            self.logger.info('Requesting for only live data')
        elif marketDataType == 2:
            self.logger.info('Requesting for frozen data whenever live not available')
        elif marketDataType == 2:
            self.logger.info('Requesting for delayed data whenever live not available')
        else:
            self.logger.info('Requesting for delayed frozen data whenever live not available')
        self.reqMarketDataType(marketDataType)

    @iswrapper
    def marketDataType(self, reqId, marketDataType):
        self.logger.info('Market Data Type successfully changed to %s' % str(marketDataType))

    # =================================  Contract details functions  =========================================

    def getMatchingSymbols(self, symbol):
        reqId = self.getNextId()
        self.reqMatchingSymbols(reqId, symbol)

    @iswrapper
    def symbolSamples(self, reqId, contractDescriptions):
        super().symbolSamples(reqId, contractDescriptions)

    # ================================  Historical data functions  ==========================================

    def getHeadTimeStampAndHistoricalData(self, contract, endDatetime, durationString, barSizeSetting,
                                        whatToShow, useRTH, formatDate, keepUpToDate, chartOptions):
        """
        This function has been built in such a way that we first call getHeadTimestamp. When the receiver function
        receives the headTimestamp, it will automatically call the getHistoricalData function.
        :param contract: (contracts.Contract) My custom-made Contract object
        :param endDatetime: (str) Can be an empty string or date string in IB's accepted date format
        :param durationString: (str) See https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration
        :param barSizeSetting: (str) See https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_barsize
        :param whatToShow: (str) https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_what_to_show
        :param useRTH: (Boolean) True or False
        :param formatDate: (int) 1 or 2
        :param keepUpToDate:
        :param chartOptions:
        :return:
        """
        reqId = self.getNextId()
        self.logger.info('#Request %d: Getting headTimestamp for %s' % (reqId, contract.symbol))
        self.historicBarHandler.createRecord(reqId, contract=contract, endDatetime=endDatetime,
                                             durationString=durationString, barSizeSetting=barSizeSetting,
                                             whatToShow=whatToShow, useRTH=useRTH, formatDate=formatDate,
                                             keepUpToDate=keepUpToDate, chartOptions=chartOptions, headTimestamp=None)
        self.reqHeadTimeStamp(reqId, contract, whatToShow, useRTH, formatDate)

    @iswrapper
    def headTimestamp(self, reqId, headTimestamp):
        super().headTimestamp(reqId, headTimestamp)
        self.logger.info('#Request %d: headTimestamp received' % reqId)
        self.historicBarHandler.editRecord(reqId, headTimestamp=headTimestamp)
        record = self.historicBarHandler.getRecord(reqId)
        self.getHistoricalData(record['contract'], record['endDatetime'], record['durationString'],
                               record['barSizeSetting'], record['whatToShow'],record['useRTH'],
                               record['formatDate'], record['keepUpToDate'], record['chartOptions'], oldReqId=reqId)

    def getHistoricalData(self, contract, endDatetime, durationString, barSizeSetting, whatToShow,
                          useRTH, formatDate, keepUpToDate, chartOptions, oldReqId=None, headTimestamp=None):

        # Getting a unique request id so responses will not be mixed up
        reqId = self.getNextId()

        if not oldReqId:

            # The oldReqId is not provided when we call this function from the outside. This is typically when we
            # already have an existing database of the prices and not parsing prices for the first time.
            if not headTimestamp:

                # Without knowing what was the most recent data date, the algorithm would stupidly parse prices
                # from the beginning again.
                raise ValueError('Either oldReqId or headTimestamp must be provided. You did not provide both.')
            else:

                # In this case, the most recent data date can be found from the sqlite3 database where our
                # prices are stored, and passed into our historicBarHandler as headTimestamp.
                self.historicBarHandler.createRecord(reqId, contract=contract, endDatetime=endDatetime,
                                                     durationString=durationString, barSizeSetting=barSizeSetting,
                                                     whatToShow=whatToShow, useRTH=useRTH, formatDate=formatDate,
                                                     keepUpToDate=keepUpToDate, chartOptions=chartOptions,
                                                     headTimestamp=headTimestamp)
                self.logger.info('#Request %d: Getting historical bars for %s from %s' %
                                 (reqId, contract.symbol, endDatetime)
                                 )
        else:

            # The oldReqId is provided if the getHeadTimestamp function is first called, as this allows us to
            # continuously call the getHistoricalData function without escaping the current namespace. Presently,
            # I have not found a way to do it properly with asyncio, but am not keen on relying on third party
            # sources such as Ewald de wit's ib_insync.
            self.historicBarHandler.reindexRecord(reqId, oldReqId)
            self.logger.info('#Request %d: Getting historical bars for %s from scratch' % (reqId, contract.symbol))

        self.reqHistoricalData(reqId, contract, endDatetime, durationString, barSizeSetting,
                               whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)

    @iswrapper
    def historicalData(self, reqId, bar):
        """

        :param reqId:
        :param bar:
        :return:
        """
        super().historicalData(reqId, bar)
        self.logger.debug('#Request %d: Received bar data for %s' % (reqId, bar.date))
        self.historicBarHandler.editRecord(reqId, bars=bar)

    @iswrapper
    def historicalDataEnd(self, reqId, start, end):
        """

        :param reqId:
        :param start:
        :param end:
        :return:
        """
        super().historicalDataEnd(reqId, start, end)
        self.logger.info('#Request %d: Received bar data from %s to %s' % (reqId, start, end))
        self.historicBarHandler.closeRecord(reqId)

        # Iterative call to getHistoricalData. This is because Interactive Brokers has a limitation to the max
        # amount of data we can parse at one go. The start date of a entire batch of parsed data can serve as
        # the new endDatetime for the next batch. This allows us to iteratively parse batches of data backwards.
        record = self.historicBarHandler.getRecord(reqId)
        headTimestampStr = record['headTimestamp']
        headTimestamp = datetime.datetime.strptime(headTimestampStr, utils.IB_DATE_FMT)

        # The day before the start date of the current batch of data will be the last date of the next batch
        nextEndDatetime = datetime.datetime.strptime(start, utils.IB_DATE_FMT) - datetime.timedelta(days=1)

        # If the last date of the next batch is bigger than the very first date of data, then there is still
        # data to be parsed. Else, there is no longer any more need for extracting data.
        if nextEndDatetime > headTimestamp:
            nextEndDatetimeStr = nextEndDatetime.strftime(utils.IB_DATE_FMT)
            self.getHistoricalData(
                record['contract'], nextEndDatetimeStr, '1 Y',
                record['barSizeSetting'], record['whatToShow'],record['useRTH'],
                record['formatDate'], record['keepUpToDate'], record['chartOptions'], headTimestamp=headTimestampStr
            )
        else:
            self.logger.info('All historical bars for %s received.' % record['contract'].symbol)

    # ====================================  Live data functions  =============================================

    def getMktData(self, contract, genericTickList='', snapshot=False, regulatorySnapshot=False, mktDataOptions=None):
        """

        :param contract:
        :param genericTickList:
        :param snapshot:
        :param regulatorySnapshot:
        :param mktDataOptions:
        :return:
        """
        reqId = self.getNextId()
        self.logger.info('#Request %d Started: Tick data for %s' % (reqId, contract.symbol))
        self.tickPriceHandler.createRecord(reqId, contract, snapshot, regulatorySnapshot, mktDataOptions)
        self.tickSizeHandler.createRecord(reqId, contract, snapshot, regulatorySnapshot, mktDataOptions)
        self.tickGenericHandler.createRecord(reqId, contract, snapshot, regulatorySnapshot, mktDataOptions)
        self.reqMktData(reqId, contract, genericTickList, snapshot, regulatorySnapshot, mktDataOptions)

    def stopMktData(self, ticker):
        """

        :param ticker:
        :return:
        """
        for reqId, nestedDict in self.tickPriceHandler:
            if nestedDict['ticker'] == ticker:
                self.tickPriceHandler.closeRecord(reqId)
                self.tickSizeHandler.closeRecord(reqId)
                self.tickGenericHandler.closeRecord(reqId)
                self.cancelMktData(reqId)
                break

    @iswrapper
    def tickPrice(self, reqId, tickType, price, attrib):
        """

        :param reqId:
        :param tickType:
        :param price:
        :param attrib:
        :return:
        """
        super().tickPrice(reqId, tickType, price, attrib)
        self.logger.debug('#Request %d In Progress: Tick Price (%d) Received' % (reqId, tickType))
        self.tickPriceHandler.refreshRecord(reqId, tickType, price, attrib)

    @iswrapper
    def tickSize(self, reqId, tickType, size):
        """

        :param reqId:
        :param tickType:
        :param size:
        :return:
        """
        super().tickSize(reqId, tickType, size)
        self.logger.debug('#Request %d In Progress: Tick Size (%d) Received' % (reqId, tickType))
        self.tickSizeHandler.refreshRecord(reqId, tickType, size)

    @iswrapper
    def tickGeneric(self, reqId, tickType, value):
        """

        :param reqId:
        :param tickType:
        :param value:
        :return:
        """
        super().tickGeneric(reqId, tickType, value)
        self.logger.debug('#Request %d In Progress: Tick Generic (%d) Received' % (reqId, tickType))
        self.tickGenericHandler.refreshRecord(reqId, tickType, value)
