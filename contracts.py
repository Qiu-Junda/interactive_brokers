from ibapi import contract


class Contract(contract.Contract):
    def __init__(self, secType, symbol, exchange, currency):
        super(Contract, self).__init__()

        if secType in ['STK', 'BOND', 'BONDS', 'OPT', 'FUT', 'CASH']:
            if secType in ['STK', 'BOND', 'BONDS', 'CASH']:
                self.symbol = symbol
            elif secType in ['OPT', 'FUT']:
                self.localSymbol = symbol
            else:
                raise NotImplementedError

        self.currency = currency
        self.secType = secType
        self.exchange = exchange
