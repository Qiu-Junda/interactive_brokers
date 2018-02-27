import utils
import logging
import sessions
import contracts


def setupLogger():
    logging.basicConfig(level=logging.INFO)
    for handler in logging.root.handlers:
        handler.addFilter(logging.Filter('Interactive Brokers'))


def extractHistoricalData(whatToShow):
    host = utils.HOST
    port = utils.PAPER_ACCT_PORT
    clientId = utils.HISTORICAL_DATA_ID

    useRTH = 1
    formatDate = 1
    chartOptions = []
    keepUpToDate = False
    durationString = '1 Y'
    barSizeSetting = '1 day'

    secTypes = ['STK'] * len(utils.SPDR_SECTOR_UNIVERSE)
    exchanges = ['ARCA'] * len(utils.SPDR_SECTOR_UNIVERSE)
    currencies = ['USD'] * len(utils.SPDR_SECTOR_UNIVERSE)
    ibContracts = [
        contracts.Contract(secType, symbol, exchange, currency) for secType, symbol, exchange, currency in
        zip(secTypes, utils.SPDR_SECTOR_UNIVERSE, exchanges, currencies)
    ]

    setupLogger()
    session = sessions.IBHistoricalDataSession(host, port, clientId)
    session.extract(ibContracts, durationString, barSizeSetting, whatToShow,
                    useRTH, formatDate, keepUpToDate, chartOptions)
