import os
import etc
import sqlite3


class Database:
    def __init__(self):
        self.filePath = ''
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = sqlite3.connect(self.filePath)
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.close()


class IBHistoricalDatabase(Database):
    def __init__(self):
        super(IBHistoricalDatabase, self).__init__()

    def insertDf(self, tblName, df):
        df.to_sql(tblName, self.connection, if_exists='append', index=False)

    def select(self, script):
        self.cursor.execute(script)
        return self.cursor.fetchall()[0]

    def insert(self, script, args):
        try:
            if isinstance(args, list):
                self.cursor.executemany(script, args)
            elif isinstance(args, tuple):
                self.cursor.execute(script, (args, ))
            else:
                raise NotImplementedError
        except:
            raise NotImplementedError


class HistoricalAdjustedLastDatabase(IBHistoricalDatabase):
    def __init__(self):
        super(HistoricalAdjustedLastDatabase, self).__init__()
        self.name = 'HistoricalAdjustedLast.db'
        self.filePath = os.path.join(etc.PATH, self.name)


class HistoricalTradesDatabase(IBHistoricalDatabase):
    def __init__(self):
        super(HistoricalTradesDatabase, self).__init__()
        self.name = 'HistoricalTrades.db'
        self.filePath = os.path.join(etc.PATH, self.name)


class HistoricalOptionImpliedVolDatabase(IBHistoricalDatabase):
    def __init__(self):
        super(HistoricalOptionImpliedVolDatabase, self).__init__()
        self.name = 'HistoricalOptionImpliedVol.db'
        self.filePath = os.path.join(etc.PATH, self.name)


class HistoricalOptionDatabase:
    def __init__(self):
        self.name = 'HistoricalOption.db'
        self.filePath = os.path.join(etc.PATH, self.name)

    def create(self):
        path = os.path.join(self.filePath, 'historical us options')
        for folder in ['2012', '2013', '2014', '2015', '2016', '2017']:
            for month in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']:
                try:
                    path = '/'.join([self.filePath, 'historical us options', folder, month])
                    files = os.listdir(path)
                except:
                    raise Exception('The path %s cannot be found' % path)
                else:
                    pass
