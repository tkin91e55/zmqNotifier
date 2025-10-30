"""Notification system for market alerts and monitoring."""

import logging
from collections import defaultdict
from collections import deque

logger = logging.getLogger(__name__)


class VolatilityNotifier:
    """
    Volatility measures ticks counts (activity of trades) and price fluctuations over
    short time windows. Short time windows are supposed moved small relative to long term,
    therefore, when short term windows exhibits comparative movements to its parent windows
    exponentials, there is a high chance that external forces are acting on the market and hence an
    intensity indicator.

    The concept:

    Record most recent ticks in some short time windows

    a. Pick a few short time windows: M1, M5, M15,
       these time windows has been chosen thresolds to be
       nofitifiable abrupt changes from past historic data.
    b. For each above mentioned time scale, there is max, min heaps to keep

    Assign a exponential
    Each TF has a volatility threshold, collect ticks into tick_deque, and
    there are pointers on the tick match the interval of each TF:

    [oldest_tick, ..., t_n-1, t_newest]  # tick_deque
    <-------------- H1 --------------->
                    <------- M5 ------>
                              <---M1-->

    - Compare against thresholds
    - Send notifications if exceeded

    The role of this class is to.

    1. keep the recent tick
    2. make calculation, current volatility for each TF,
      * should use max-min or new-old for each TF? or new-extreme?
      * I am not sure, mabe ARIMA or Brownian Motion have better rationales to
        to determine is over a period of ticks, there is external force on the TF periods.
    3. notify when certain conditions met
    4. and scores of exceed
    """

    # Possible to use Telegram?
    # read from json and get symbols and their thresholds for highlow and vol in TFs
    WATCHED_TFs = ["M1", "M5", "H1"]
    recent_ohlc = defaultdict(list)  # symbol_tf -> deque
    tick_deque = deque()

    def __init__(self):
        pass

    def on_tick(self, tick):
        """
        Process incoming tick data.

        :param tick: Incoming tick data.
        """
        self.tick_deque.append(tick)
        # Further processing to be implemented


from abc import ABC, abstractmethod


class NotifierBackend(ABC):
    """Abstract base class for notification backends."""

    @abstractmethod
    async def send_message(self, message: str) -> bool:
        """
        Send a notification message.

        Parameters
        ----------
        message : str
            The message to send

        Returns
        -------
        bool
            True if the message was sent successfully, False otherwise

        """
        pass
