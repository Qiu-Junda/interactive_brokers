import utils


def checkAmbiguousContracts(cds):
    if len(cds) > 1:
        raise AssertionError('Contract for %s is ambiguous.' % (cds[0],))


def checkFxDetails(symbol, currency):
    if symbol in utils.CURRENCIES_PRIOR_TO_USD or currency in utils.CURRENCIES_AFTER_USD:
        return symbol, currency
    elif symbol in utils.CURRENCIES_AFTER_USD or currency in utils.CURRENCIES_PRIOR_TO_USD:
        return currency, symbol
    elif symbol == currency:
        return symbol, currency
