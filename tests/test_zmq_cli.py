# ruff: noqa
import unittest
from pathlib import Path

# run: python -m unittest zmqNotifier.zmq_cli_test

class TestZMQMT4(unittest.TestCase):

    @classmethod
    def setUpclass(cls):
        from zmqNotifier.zmq_cli import ZMQ_MT4
        cls.subscriber = ZMQ_MT4()
        import pickle
        with open('zmqNotifier/data/zmq_market_db.pickle','rb') as pf:
            cls.db = pickle.load(pf)

    def test_validate_onebaronetick(self):
        pass
