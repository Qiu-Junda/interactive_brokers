import os
import utils
import sqlite3
import pandas as pd
from abc import ABCMeta, abstractmethod


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")


class HistoricCSVDataHandler(DataHandler):
    def __init__(self, event_queue, csv_dir, symbol_list):
        self.event_queue = event_queue
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        comb_idx = None
        for s in self.symbol_list:
            self.symbol_data[s] = pd.read_csv(self.csv_dir, index_col=0, parse_dates=[0], dayfirst=True)
            if not comb_idx:
                comb_idx = self.symbol_data[s].index
            else:
                comb_idx.union(self.symbol_data[s].index)

            self.latest_symbol_data[s] = []

        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_idx, method='pad').iterrows()

    def _get_new_bar(self, symbol):
        for b in self.symbol_data[symbol]:
            yield b

    def get_latest_bars(self, symbol, num_bars=1):
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            raise KeyError('Symbol %s is not available' % symbol)
        else:
            return bars_list[-num_bars:]

    def update_bars(self):
        for s in self.symbol_list:
            try:
                bar = self._get_new_bar(s)
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)


class IBHistoricBarHandler:
    def __init__(self):
        self.records = {}

    def __iter__(self):
        for reqId, nestedDict in self.records.items():
            yield reqId, nestedDict

    def getRecord(self, reqId):
        return self.records[reqId]

    def reindexRecord(self, newReqId, oldReqId):
        self.records[newReqId] = self.records.pop(oldReqId)

    def editRecord(self, reqId, **kwargs):
        for key, arg in kwargs.items():
            if key != 'bars':
                self.records[reqId][key] = arg
            else:
                self.records[reqId][key].append(
                    pd.DataFrame(
                        index=['date', 'open', 'high', 'low', 'close', 'volume', 'barCount', 'average'],
                        data=[arg.date, arg.open, arg.high, arg.low, arg.close, arg.volume, arg.barCount, arg.average]
                    )
                )

    def createRecord(self, reqId, **kwargs):
        """
        The keywords to be used are these below
        :param reqId:
        :param contract:
        :param endDatetime:
        :param durationString:
        :param barSizeSetting:
        :param whatToShow:
        :param useRTH:
        :param formatDate:
        :param keepUpToDate:
        :param chartOptions:
        :param headTimestamp:
        :return:
        """
        self.records[reqId] = {key: arg for key, arg in kwargs.items()}
        self.records[reqId]['bars'] = []

    def closeRecord(self, reqId):
        whatToShow = self.records[reqId]['whatToShow']
        ticker = self.records[reqId]['contract'].symbol
        records = pd.concat(self.records[reqId]['bars'], axis=1, join='outer').T.iloc[::-1]
        records.to_sql(name=ticker, index=False, if_exists='append',
                       con=sqlite3.connect(os.path.join(utils.PATH, utils.determineDatabaseToUse(whatToShow))))


class IBTickPriceHandler:
    def __init__(self):
        self.records = {}

    def __iter__(self):
        for reqId, nestedDict in self.records.items():
            yield reqId, nestedDict

    def createRecord(self, reqId, contract, snapshot, regulatorySnapshot, mktDataOptions):
        self.records[reqId] = {'contract': contract, 'last': None, 'bid': None, 'ask': None}

    def refreshRecord(self, reqId, tickType, price, attrib):
        if tickType == utils.BID_PRICE_TICK_TYPE:
            self.records[reqId]['bid'] = price
        elif tickType == utils.ASK_PRICE_TICK_TYPE:
            self.records[reqId]['ask'] = price
        elif tickType == utils.LAST_PRICE_TICK_TYPE:
            self.records[reqId]['last'] = price
        else:
            if not self.records[reqId]['last']:
                if tickType == utils.LAST_RTH_TRADED_PRICE:
                    self.records[reqId]['last'] = price

                if not self.records[reqId]['last']:
                    if tickType == utils.CLOSE_PRICE_TICK_TYPE:
                        self.records[reqId]['last'] = price

    def closeRecord(self, reqId):
        del self.records[reqId]


class IBTickSizeHandler:
    def __init__(self):
        self.records = {}

    def __iter__(self):
        for reqId, nestedDict in self.records.items():
            yield reqId, nestedDict

    def createRecord(self, reqId, contract, snapshot, regulatorySnapshot, mktDataOptions):
        self.records[reqId] = {'contract': contract, 'last': None, 'bid': None, 'ask': None}

    def refreshRecord(self, reqId, tickType, size):
        if tickType == utils.BID_SIZE_TICK_TYPE:
            self.records[reqId]['bid'] = size
        elif tickType == utils.ASK_SIZE_TICK_TYPE:
            self.records[reqId]['ask'] = size
        elif tickType == utils.LAST_SIZE_TICK_TYPE:
            self.records[reqId]['last'] = size
        else:
            pass

    def closeRecord(self, reqId):
        del self.records[reqId]


class IBTickGenericHandler:
    def __init__(self):
        self.records = {}

    def __iter__(self):
        for reqId, nestedDict in self.records.items():
            yield reqId, nestedDict

    def createRecord(self, reqId, contract, snapshot, regulatorySnapshot, mktDataOptions):
        self.records[reqId] = {'contract': contract}

    def refreshRecord(self, reqId, tickType, value):
        self.records[reqId][tickType] = value

    def closeRecord(self, reqId):
        del self.records[reqId]
