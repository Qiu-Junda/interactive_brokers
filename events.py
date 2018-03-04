import pandas as pd
from abc import ABCMeta


class Event:

    __metaclass__ = ABCMeta

    def __eq__(self, other):
        for key, arg in self.__dict__:
            if not self[key] == other[key]:
                return False
        return True


class HistoricalDataEvent(Event):
    def __init__(self, contract, endDateTime, durationString, barSizeSetting, whatToShow,
                 useRTH, formatDate, keepUpToDate, chartOptions, headTimestamp, bars):
        self.contract = contract
        self.endDateTime = endDateTime
        self.durationString = durationString
        self.barSizeSetting = barSizeSetting
        self.whatToShow = whatToShow
        self.useRTH = useRTH
        self.formatDate = formatDate
        self.keepUpToDate = keepUpToDate
        self.chartOptions = chartOptions
        self.headTimestamp = headTimestamp
        self.bars = bars

    def edit(self, kwargs):
        for key, arg in kwargs.items():
            if key == 'headTimestamp':
                self.headTimestamp = arg
            elif key == 'endDateTime':
                self.endDateTime = arg
            elif key == 'bars':
                self.bars.append(
                    pd.DataFrame(
                        index=['date', 'open', 'high', 'low', 'close', 'volume', 'barCount', 'average'],
                        data=[arg.date, arg.open, arg.high, arg.low, arg.close, arg.volume, arg.barCount, arg.average]))


class TickEvent(Event):
    def __init__(self, contract, genericTickList, snapshot, regulatorySnapshot, mktDataOptions):
        self.contract = contract
        self.snapshot = snapshot
        self.genericTickList = genericTickList
        self.regulatorySnapshot = regulatorySnapshot
        self.mktDataOptions = mktDataOptions

    def edit(self, **kwargs):
        for key, arg in kwargs.items():
            self[key] = arg
