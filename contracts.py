import utils
from ibapi import contract


class Contract(contract.Contract):
    def __init__(self, secType, symbol, exchange, currency):
        super(Contract, self).__init__()

        if secType in ['STK', 'BOND', 'BONDS', 'OPT', 'FUT']:
            if secType in ['STK', 'BOND', 'BONDS']:
                self.symbol = symbol
            elif secType in ['OPT', 'FUT']:
                self.localSymbol = symbol
            else:
                raise NotImplementedError
            self.currency = currency

        elif secType == 'CASH':
            if symbol in utils.CURRENCIES_PRIOR_TO_USD or currency in utils.CURRENCIES_AFTER_USD:
                self.symbol = symbol
                self.currency = currency
            elif symbol in utils.CURRENCIES_AFTER_USD or currency in utils.CURRENCIES_PRIOR_TO_USD:
                self.symbol = currency
                self.currency = symbol
            elif symbol == currency:
                self.symbol = symbol
                self.currency = currency
            else:
                raise NotImplementedError('Symbol %s Currency %s is not supported.' % (symbol, currency))
        else:
            raise NotImplementedError('The asset type %s is not supported' % secType)

        self.secType = secType
        self.exchange = exchange

    def getSymbol(self):
        return self.symbol if self.symbol else self.localSymbol


class IBContractHandler:
    def __init__(self):
        self.records = {}

    def createRecord(self, reqId):
        self.records[reqId] = []

    def appendRecord(self, reqId, contractDetails):
        self.records[reqId].append(contractDetails)

    def closeRecord(self, reqId):
        for i, c in enumerate(self.records[reqId]):
            print(str(i), ':',
                  'Symbol - %s' % c.getSymbol(),
                  'SecType - %s' % c.secType,
                  'Exchange - %s' % c.exchange,
                  'Currency - %s' % c.currency,
                  '\n')

        idx = None
        while not isinstance(idx, int):
            idx = input('Which contract do you want? Enter the corresponding number: ')
            if 0 <= int(idx) < len(self.records[reqId]):
                idx = int(idx)

        self.records[reqId] = self.records[reqId][idx]
        return
