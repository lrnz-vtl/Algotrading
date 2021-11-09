from trade_logger.base import TradeLog, TradeLogger
from pathlib import Path
from dataclasses import asdict


class TextLogger(TradeLogger):
    '''
    Crappy logger
    '''

    def __init__(self, fname:str):
        self.fname = fname
        Path(fname).touch()

    def log(self, trade: TradeLog):
        with open(self.fname, 'a') as f:
            f.write(str(asdict(trade)))