# TODO run a daemon background process/service
#      https://discuss.python.org/t/proper-way-to-create-a-daemon-process/79824/
#      https://pyzmq.readthedocs.io/en/latest/howto/eventloop.html
from datetime import datetime
from zmqNotifier.DWX_ZeroMQ_Connector_v2_0_1_RC8 import DWX_ZeroMQ_Connector

"""
On Dev, client side on macOS and connect to MT4 server side on Windows 10 (Parallels).
Usage:
    from zmqNotifier.zmq_cli import ZMQ_MT4
    z2 = ZMQ_MT4(_host='10.211.55.4')
    z2.subscribe('BTCUSD')
    z2.track_rates('BTCUSD',['M1'])

For convenience, use autoreload in ipython
In [17]: %load_ext autoreload
In [18]: %autoreload 2
"""

SUPPORTED_SYMBOLS = [
        # The 28 ccys + gold + bitcoin + oil
        "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD",
        "USDCHF", "USDJPY", "EURAUD", "EURCAD", "EURCHF",
        "EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD",
        "GBPCHF", "GBPJPY", "GBPNZD", "AUDCAD", "AUDCHF",
        "AUDJPY", "AUDNZD", "NZDCAD", "NZDCHF", "NZDJPY",
        "CADCHF", "CADJPY", "CHFJPY", "BTCUSD", "XAUUSD", "USOUSD" ]

def row_bar(data,logger=None):
    for utc_time, d in data.items():
        broker_time, O, H, L, C, V, *_ = d
        dt = datetime.fromtimestamp(broker_time)
        print(f'now local: {datetime.now()}, utc: {utc_time} , broker time:{dt}'
              f' {O=},{H=},{L=},{C=},{V=}')
        # now local: 2025-10-08 22:59:00.377130, 2025-10-08 14:59:00.376870 ,2025-10-09 01:58:00

def row_tick(data,logger=None):
    for utc_time, (bid, ask) in data.items():
        print(f'now local: {datetime.now()}, utc: {utc_time}, {bid=}, {ask=}')

class ZMQ_MT4(DWX_ZeroMQ_Connector):
    """
    Client to the server MT4 side

    Example usage:

    z = ZMQ_MT4(_host="xxx")
    z.subscribe('BTCUSD_M1')
    # or z.subscribe('BTCUSD') # including bars and ticks
    z.unsubscribe('BTCUSD_M1')
    z.shutdown()
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._subdata_handlers.append(self.on_data_recv)
        self.log_enabled = False # each symbol has its own logger
        self.loggers = dict()
        # self.logger = DataLogger(self)
        self.notifier = VolatilityNotifier(self)
        self._verbose = False

    @property
    def is_active(self):
        return self._ACTIVE

    @property
    def verbose(self):
        return self._verbose

    @verbose.setter
    def verbose(self, val: bool):
        self._verbose = val

    @property
    def market_data(self):
        """
        A dictionary, {
             'symbol':  {time_key and values}, # as tick data
             'symbol_tf': {time_key and values},
             'symbol_tf2': {time_key, and values}
        }
        """
        return self._Market_Data_DB

    def subscribe(self, symbol, for_tickdata=False, for_ohlc_m1=False):
        """
        Creating channel in MT4 ZMQ server, there is no UI way to delete the created channel.
        The Symbol channel will then stream the tick data and OHLC to subscribes of the symbol
        1. After subscribing a symbol, need to explicity request for tick data and/or OHLC data
        2. for ohlc if TFs, TRACKRATES can only activate multiple simultaneously TF M1, M5...
           No, but passing a list of timeframes. The calling is not additive, but idempotent
        """
        self._DWX_MTX_SUBSCRIBE_MARKETDATA_(symbol)
        if for_tickdata:
            self._DWX_MTX_SEND_TRACKPRICES_REQUEST_(symbol)
        if for_ohlc_m1:
            # it acutally take format: channel_name, symbol, timeframe in minutes
            # channel name appear to be the _M
            self._DWX_MTX_SEND_TRACKRATES_REQUEST_([f'{symbol}_M1',f'{symbol}', 1])

    def unsubscribe(self, symbol):
        """
        There is not way to unsubscribe for a sub-channel (ohlc in TFs, tick data)
        Restart the server, or there is some commands?
        """
        self._DWX_MTX_UNSUBSCRIBE_MARKETDATA_(symbol)

    def track_ticks(self, symbol):
        """called after subscribe()"""
        self._DWX_MTX_SEND_TRACKPRICES_REQUEST_([symbol])

    def track_rates(self, symbol, timeframes=None):
        """
        called after subscribe()
        timeframes: list of str, e.g. ['M1','M5','H1','D1']
        """
        if not timeframes: timeframes = []
        the_map = { 'M1': 1, 'M5': 5, 'M15': 15, 'M30': 30,
                    'H1': 60, 'H4': 240, 'D1': 1440, 'W1': 10080, 'MN': 43200 }
        sub_channels = []
        for tf in timeframes:
            if tf not in the_map:
                raise ValueError(f'Timeframe {tf} not supported.')
            sub_channels.append((f'{symbol}_{tf}',f'{symbol}', the_map[tf]))
        self._DWX_MTX_SEND_TRACKRATES_REQUEST_(sub_channels)

    def on_data_recv(self,_):
        """
        Refer to _subdata_handlers, it is individual thread execution.
        The meachanism is using a polling with poll_delay=0.001 second loop

        The parsed data is stored in self._Market_Data_DB, an entry is like this:
        {'2025-10-09 06:32:00.259239':
            (1760002260, 122116.68, 122179.17, 122115.79, 122170.36, 89, 0, 0)}
        where the key is UTC time by Mao, and 1760002260 is the broker time in seconds, and
        OHLCV follows.

        Or tick data:
        {'2025-10-09 05:52:24.825553': (121989.53, 122006.49)}
        """
        try:
            assert len(self._Market_Data_DB) > 0, "[Warning] Expected callback one data at a time"
        except AssertionError as e:
            print(e)
        for channel in self._Market_Data_DB:
            try:
                symbol, tf = channel.split('_')
                row_func = row_bar
            except ValueError:
                symbol, tf = channel, 'tick'
                row_func = row_tick
            data = self._Market_Data_DB[channel]
            row_func(data)
        # Clear the Market Data, it has classified the data channel
        self._Market_Data_DB.clear()

    def shutdown(self):
        self._DWX_ZMQ_SHUTDOWN_()
        # TODO deal with loggers too?


from collections import deque
from collections import defaultdict
class VolatilityNotifier:
    """
    The role of this class is to
    1. keep the recent tick and bars,
    2. make calculation
    3. notify when certain conditions met
    # TODO added own data handler, deque for window calculation of M1
    """
    # Possible to use Telegram?
    # read from json and get symbols and their thresholds for highlow and vol in TFs
    WATCHED_TFs = ['M1','M5','H1']
    recent_ohlc = defaultdict(list) # symbol_tf -> deque
    tick_deque = deque()

    def __init__(self, proxy: ZMQ_MT4):
        self._proxy = proxy
