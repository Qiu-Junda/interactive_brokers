import etc
import pytz
import events
import logging
import handlers
import databases
from ibapi.client import EClient
from ibapi.utils import iswrapper
from ibapi.wrapper import EWrapper
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def getPrevWeekday(date):
    date -= timedelta(days=1)
    while date.weekday() > 4: # Mon-Fri are 0-4
        date -= timedelta(days=1)
    return date


class BacktestSession:
    def __init__(self):
        raise NotImplementedError


class IBHistoricalDataSession(EWrapper, EClient):
    def __init__(self, host, port, clientId):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.connect(host, port, clientId)
        logger.info('Interactive Brokers is connected.')

        self.reqId = 0
        self.historicBarHandler = handlers.IBHistoricBarHandler()

    def getNextId(self):
        self.reqId += 1
        return self.reqId

    def start(self, contracts, durationString, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions):
        """

        :param contracts: (list of contracts.Contract)
        :param durationString: (str) See https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration
        :param barSizeSetting: (str) See https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_barsize
        :param whatToShow: (str) https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_what_to_show
        :param useRTH: (Boolean) True or False
        :param formatDate: (int) 1 or 2
        :param keepUpToDate: (boolean) True or False
        :param chartOptions: (list)
        :return:
        """
        logger.info('HistoricalDataSession started.')

        # Initializing databases to check what existing historical data we already have
        if whatToShow == 'ADJUSTED_LAST':
            db = databases.HistoricalAdjustedLastDatabase()
        elif whatToShow == 'TRADES':
            db = databases.HistoricalTradesDatabase()
        else:
            raise NotImplementedError

        db.connect()
        endDateTime = ''

        for contract in contracts:
            script = "SELECT date FROM %s DESC LIMIT 1" % contract.symbol
            selected = db.select(script)
            if not selected:

                # If no existing table, search IB for the earliest data point
                logger.debug('No existing sqlite table for %s' % contract.symbol)
                self.getHeadTimeStamp(contract, endDateTime, durationString, barSizeSetting, whatToShow,
                                      useRTH, formatDate, keepUpToDate, chartOptions)
            else:

                # Else, iterate over the headTimestamps (although there is only one)
                for headTimestamp in db.select(script):
                    logger.debug('Symbol %s headTimestamp %s' % (contract.symbol, headTimestamp))

                    easternDateTime = datetime.now(pytz.timezone('US/Eastern'))
                    prevEasternWeekday = getPrevWeekday(easternDateTime)
                    easternHeadTimestamp = pytz.timezone('US/Eastern').localize(
                        datetime.strptime(headTimestamp, etc.IB_BAR_DATE_FMT))
                    daysDiff = (prevEasternWeekday - easternHeadTimestamp).days
                    if daysDiff > 0:
                        daysDiffStr = str(daysDiff)
                        logger.info(
                            'Most recent historicalData for %s is %s. Parsing %s days of historicalData' %
                            (contract.symbol, easternHeadTimestamp.strftime(etc.IB_BAR_DATE_FMT), daysDiffStr))
                        self.getHistoricalData(contract, endDateTime, daysDiffStr + ' D', barSizeSetting, whatToShow,
                                               useRTH, formatDate, keepUpToDate, chartOptions)
                    else:
                        logger.info(
                            'Most recent historicalData for %s is %s. No historicalData to be parsed' %
                            (contract.symbol, easternHeadTimestamp.strftime(etc.IB_BAR_DATE_FMT)))

        # Either way, close the database and start the event loop in IB
        db.close()
        self.run()

    def getHeadTimeStamp(self, contract, endDateTime, durationString, barSizeSetting, whatToShow,
                         useRTH, formatDate, keepUpToDate, chartOptions):
        """
        Calls the reqHeadTimeStamp method which makes a request to IB's server
        :param contract: (contracts.Contract) My custom-made Contract object
        :param endDateTime: (str) Can be an empty string or date string in IB's accepted date format
        :param durationString: (str) See https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration
        :param barSizeSetting: (str) See https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_barsize
        :param whatToShow: (str) https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_what_to_show
        :param useRTH: (Boolean) True or False
        :param formatDate: (int) 1 or 2
        :param keepUpToDate: (boolean) True or False
        :param chartOptions: (list)
        :return: Nothing
        """
        reqId = self.getNextId()

        # Create a HistoricalDataEvent object
        logger.debug('Creating historicalDataEvent')
        historicalDataEvent = events.HistoricalDataEvent(
            contract, endDateTime, durationString, barSizeSetting, whatToShow,
            useRTH, formatDate, keepUpToDate, chartOptions, None, [])
        logger.debug('historicalDataEvent created')

        # Creating record in the handler
        logger.debug('#Request %d: Creating historicalDataEvent record for %s' % (reqId, contract.symbol))
        self.historicBarHandler.createRecord(reqId, historicalDataEvent)
        logger.debug('#Request %d: historicalDataEvent record for %s created' % (reqId, contract.symbol))

        logger.info('#Request %d: Requesting headTimestamp for %s' % (reqId, contract.symbol))
        self.reqHeadTimeStamp(reqId, contract, whatToShow, useRTH, formatDate)

    @iswrapper
    def headTimestamp(self, reqId, headTimestamp):
        super().headTimestamp(reqId, headTimestamp)

        # Edit the HistoricalDataEvent with the new value obtained
        logger.debug('Editing historicalDataEvent record with new headTimestamp')
        self.historicBarHandler.editRecord(reqId, headTimestamp=headTimestamp)
        logger.debug('historicalDataEvent record with new headTimestamp edited')

        # Get event object which holds information on required params for calling reqHistoricalData
        logger.debug('#Request %d: Getting historicalDataEvent record' % reqId)
        event = self.historicBarHandler.getRecord(reqId)
        logger.debug('#Request %d: historicalDataEvent record gotten' % reqId)

        logger.info('#Request %d: Requesting historicalData for %s' % (reqId, event.contract.symbol))
        self.getHistoricalData(event.contract, event.endDateTime, event.durationString, event.barSizeSetting,
                               event.whatToShow, event.useRTH, event.formatDate, event.keepUpToDate,
                               event.chartOptions, reqId)

    def getHistoricalData(self, contract, endDateTime, durationString, barSizeSetting, whatToShow,
                          useRTH, formatDate, keepUpToDate, chartOptions, oldReqId=None):
        """
        Sends a historical data request to IB's server
        :param contract: (contracts.Contract) My custom-made Contract object
        :param endDateTime: (str) Can be an empty string or date string in IB's accepted date format
        :param durationString: (str) See https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_duration
        :param barSizeSetting: (str) See https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_barsize
        :param whatToShow: (str) https://interactivebrokers.github.io/tws-api/historical_bars.html#hd_what_to_show
        :param useRTH: (Boolean) True or False
        :param formatDate: (int) 1 or 2
        :param keepUpToDate: (boolean) True or False
        :param chartOptions: (list)
        :param oldReqId:
        :return:
        """
        reqId = self.getNextId()
        if oldReqId:

            # Means my handler already has an existing event
            logger.debug('Reindexing historicalDataEvent record')
            self.historicBarHandler.reindexRecord(reqId, oldReqId)
            logger.debug('historicalDataEvent record reindexed')

        else:

            # Create a HistoricalDataEvent object
            logger.debug('Creating a historicalDataEvent')
            historicalDataEvent = events.HistoricalDataEvent(
                contract, endDateTime, durationString, barSizeSetting, whatToShow,
                useRTH, formatDate, keepUpToDate, chartOptions, None, [])
            logger.debug('historicalDataEvent created')

            # Creating a new event in our handler
            logger.debug('#Request %d: Creating historicalDataEvent record for %s' % (reqId, contract.symbol))
            self.historicBarHandler.createRecord(reqId, historicalDataEvent)
            logger.debug('#Request %d: historicalDataEvent record for %s created' % (reqId, contract.symbol))

        logger.info('#Request %d: Requesting historicalData for %s' % (reqId, contract.symbol))
        self.reqHistoricalData(reqId, contract, endDateTime, durationString, barSizeSetting,
                               whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)

    @iswrapper
    def historicalData(self, reqId, bar):
        """
        Wrapper which receives the historical data from IB's server
        :param reqId: (int)
        :param bar: (BarData)
        :return: Nothing
        """
        super().historicalData(reqId, bar)

        logger.debug('#Request %d: historicalData bar received. Editing historicalDataEvent record' % reqId)
        self.historicBarHandler.editRecord(reqId, bars=bar)
        logger.debug('#Request %d: historicalDataEvent record edited' % reqId)

    @iswrapper
    def historicalDataEnd(self, reqId, start, end):
        """
        Signifies the end of a batch of data. However, it will check the earliest data point it should parse.
        If the next batch will be prior to this earliest data point, then stop. Else call getHistoricalData again.
        :param reqId: (int)
        :param start: (str)
        :param end: (str)
        :return: Nothing
        """
        super().historicalDataEnd(reqId, start, end)
        logger.info('#Request %d: One batch of historicalData from %s to %s received' % (reqId, start, end))

        logger.debug('#Request %d: Saving new batch of historicalData bars' % reqId)
        self.historicBarHandler.closeRecord(reqId)
        logger.debug('#Request %d: New batch of historicalData bars saved' % reqId)

        event = self.historicBarHandler.getRecord(reqId)
        headTimestampStr = event.headTimestamp
        headTimestamp = datetime.strptime(headTimestampStr, etc.IB_DATE_FMT)
        nextEndDatetime = datetime.strptime(start, etc.IB_DATE_FMT) - timedelta(days=1)
        nextEndDatetimeStr = nextEndDatetime.strftime(etc.IB_DATE_FMT)
        self.historicBarHandler.editRecord(reqId, endDateTime=nextEndDatetimeStr)

        if nextEndDatetime > headTimestamp:
            logger.info(
                'Continuing to getHistoricalData for %s with new nextEndDateTimeStr = %s since headTimestamp = %s' %
                (event.contract.symbol, nextEndDatetimeStr, headTimestampStr)
            )

            self.getHistoricalData(
                event.contract, nextEndDatetimeStr, event.durationString,
                event.barSizeSetting, event.whatToShow, event.useRTH, event.formatDate,
                event.keepUpToDate, event.chartOptions, reqId)
        else:
            logger.info(
                'No more historicalData for %s since new nextEndDateTimeStr = %s and headTimestamp = %s ' %
                (event.contract.symbol, nextEndDatetimeStr, headTimestampStr)
            )


class IBLiveSession(EClient, EWrapper):
    def __init__(self, host, port, clientId):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.connect(host, port, clientId)

        self.reqId = 0
        self.tickHandler = handlers.IBTickHandler()

    def getNextId(self):
        self.reqId += 1
        return self.reqId

    def getMktData(self, contract, genericTickList, snapshot, regulatorySnapshot, mktDataOptions):
        reqId = self.getNextId()
        tickEvent = events.TickEvent(contract=contract, genericTickList=genericTickList, snapshot=snapshot,
                                     regulatorySnapshot=regulatorySnapshot, mktDataOptions=mktDataOptions)
        self.tickHandler.createRecord(reqId, tickEvent)
        self.reqMktData(reqId, contract, genericTickList, snapshot, regulatorySnapshot, mktDataOptions)

    @iswrapper
    def tickPrice(self, reqId, tickType, price, attrib):
        super().tickPrice(reqId, tickType, price, attrib)


    @iswrapper
    def tickSize(self, reqId, tickType, size):
        super().tickSize(reqId, tickType, size)

    @iswrapper
    def tickGeneric(self, reqId, tickType, value):
        super().tickGeneric(reqId, tickType, value)
