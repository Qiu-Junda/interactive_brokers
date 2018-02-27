import utils
import logging
import sessions
import contracts
import pandas as pd
import kristal_functions


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


def calculateKristalNav(modelPortfolios):
    host = utils.HOST
    port = utils.PAPER_ACCT_PORT  # kristal_functions.ASHEESH_ACCT_PORT
    clientId = utils.NAV_ID

    setupLogger()
    session = sessions.IBCalculateKristalNavSession(host, port, clientId)
    session.calculateNav(modelPortfolios)


if __name__ == '__main__':
    modelPortfolios = pd.read_excel(r'C:\Users\Junda\Downloads\ModelAccountPosition.xlsx', sheetname=None, index_col=0)
    calculateKristalNav(modelPortfolios)
