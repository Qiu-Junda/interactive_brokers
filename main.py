import etc
import sessions
import contracts


def extractHistoricalData(whatToShow):
    host = etc.HOST
    port = etc.PAPER_ACCT_PORT
    clientId = etc.HISTORICAL_DATA_ID

    useRTH = 1
    formatDate = 1
    chartOptions = []
    keepUpToDate = False
    durationString = '1 Y'
    barSizeSetting = '1 day'

    secTypes = ['STK'] * len(etc.SPDR_SECTOR_UNIVERSE)
    exchanges = ['ARCA'] * len(etc.SPDR_SECTOR_UNIVERSE)
    currencies = ['USD'] * len(etc.SPDR_SECTOR_UNIVERSE)
    ibContracts = [
        contracts.Contract(secType, symbol, exchange, currency) for secType, symbol, exchange, currency in
        zip(secTypes, etc.SPDR_SECTOR_UNIVERSE, exchanges, currencies)
    ]

    session = sessions.IBHistoricalDataSession(host, port, clientId)
    session.start(ibContracts, durationString, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)


if __name__ == '__main__':
    extractHistoricalData('TRADES')
