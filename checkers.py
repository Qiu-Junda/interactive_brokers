# ==============================  Other checking functions  ============================================


def check_kristal_name(kristal_name):
    return kristal_name


def check_ambiguous_contracts(cds):
    if len(cds) > 1:
        raise AssertionError('Contract for %s is ambiguous.' % (cds[0],))


# ===============================  Equity checking functions  ===========================================


def _change_chinese_stocks(symbol, exchange):
    symbol = str(symbol).zfill(6)
    if exchange == 'XSHE':
        exchange = 'SEHKSZSE'
    elif exchange == 'XSHG':
        exchange = 'SEHKNTL'
    else:
        raise NotImplementedError('Error in changing chinese stocks - Symbol: %s, Exchange: %s' % (symbol, exchange))
    return symbol, exchange


def _change_european_stocks(symbol, exchange):
    exchange = 'SMART'
    if symbol == '0NWD':
        symbol = 'ABE'
    elif symbol == 'Bitcoin XBTE':
        exchange = 'SFB'
    elif symbol == 'NVDA':
        exchange = 'BVME'
    else:
        pass
    return symbol, exchange


def _change_danish_stocks(symbol, exchange):
    exchange = 'CPH'
    if symbol == '0NVC':
        symbol = 'DANSKE'
    elif symbol == '0QIU':
        symbol = 'NOVO B'
    else:
        raise NotImplementedError('Error in changing danish stocks - Symbol: %s, Exchange: %s' % (symbol, exchange))
    return symbol, exchange


def _change_norwegian_stocks(symbol, exchange):
    exchange = 'OSE'
    if symbol == 'MHGFAD':
        symbol = 'MHG'
    else:
        raise NotImplementedError('Error in changing norwegian stocks - Symbol: %s, Exchange: %s' % (symbol, exchange))
    return symbol, exchange


def check_equity_details(symbol, exchange, currency):
    if currency in ['USD', 'CAD', 'GBP', 'JPY', 'CHF', 'AUD']:
        exchange = 'SMART'
    elif currency == 'HKD':
        exchange = 'SEHK'
    elif currency == 'SGD':
        exchange = 'SGX'
    elif currency == 'INR':
        exchange = 'NSE'
    elif currency == 'EUR':
        symbol, exchange = _change_european_stocks(symbol, exchange)
    elif currency == 'DKK':
        symbol, exchange = _change_danish_stocks(symbol, exchange)
    elif currency == 'CNY':
        currency = 'CNH'
        symbol, exchange = _change_chinese_stocks(symbol, exchange)
    elif currency == 'NOK':
        symbol, exchange = _change_norwegian_stocks(symbol, exchange)
    elif currency == 'SEK':
        exchange = 'SFB'
    else:
        raise NotImplementedError('Checking equity details - This currency %s is not supported' % currency)
    return symbol, exchange, currency


# =======================================  Bond checking functions  ===========================================


def check_bond_details(symbol, exchange, currency):
    exchange = 'SMART'
    return symbol, exchange, currency


# ======================================  Option checking functions  ===========================================


def _change_special_options(symbol, exchange):
    if 'DBK' in symbol:
        exchange = 'DTB'
    elif 'CSGN' in symbol or 'UBSN' in symbol:
        exchange = 'SOFFEX'
    else:
        raise NotImplementedError
    return symbol, exchange


def check_option_details(symbol, exchange, currency):
    if currency == 'USD':
        if 'ES' in symbol:
            exchange = 'GLOBEX'
        else:
            exchange = 'SMART'
    elif currency == 'HKD':
        exchange = 'SEHK'
    elif currency == 'INR':
        exchange = 'NSE'
    elif currency in ['EUR', 'CHF']:
        symbol, exchange = _change_special_options(symbol, exchange)
    else:
        raise NotImplementedError('Checking option details - This currency %s is not supported' % currency)
    return symbol, exchange, currency


# ===================================  Futures checking functions  ===========================================


def check_futures_details(symbol, exchange, currency):
    return symbol, exchange, currency
