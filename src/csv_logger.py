import zipfile
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from random import randint

# TODO or should use SQLite? Is compressed csv larger than compressed SQLite?
#      Considering that SQLite has less overhead given proper schema

DATA_PATH = Path(__file__) / "data"

def _ensure_parent_dir(dir):
    if not dir.exists():
        dir.mkdir(parents=True)

def _ensure_header(fn:Path,header):
    if not fn.exists():
        with open(fn,"w",encoding="utf-8") as f:
            f.write(header)

def _zip_old_logs(dir: Path, output_zip: Path):
    """Zip all CSV files in the given directory into a single zip archive."""
    if not dir.exists(): return

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for csv_file in dir.glob("*.csv"):
            zipf.write(csv_file, arcname=csv_file.name)
            csv_file.unlink()


from collections import defaultdict


class DataLogger:
    """
    The role of this class is.

    1. to log-rotate the tickdata and ohlc data (maybe keep for a period)
      * The logs are organized in DATA_PATH/symbol/yyyy_mm/symbol_tf_yyyy-mm-dd.csv
    2. keep checking abnormality of streamed data
    """

    # TODO logorate tickdata in day, zipped, by months in folder
    # use gzip, write tests

    def __init__(self, proxy, symbol, tf):
        """
        Proxy: ZMQ_MT4.

        tf: 'tick' or 'M1', 'M5', 'H1', 'D1', etc.
        """
        self._proxy = proxy
        self.symbol = symbol
        self.tf = tf
        self.nxt_flush_time = datetime.now() + timedelta(minutes=randint(3,7))

    def _new_file_handler(self, date_utc):
        # date_utc: str, in 'yyyy-mm-dd' the UTC string of the data, not the local time
        today = datetime(year=int(date_utc[0:4]),
                         month=int(date_utc[5:7]),
                         day=int(date_utc[8:10]))
        parent_dir = DATA_PATH / self.symbol / f"{today.year:04d}_{today.month:02d}"

        _ensure_parent_dir(parent_dir)
        filename = parent_dir / f"{self.symbol}_{self.tf}_{today.strftime('%Y-%m-%d')}.csv"
        if self.tf != "tick":
            header = "datetime,open,high,low,close,volume\n"
        else:
            header = "datetime,bid,ask\n"
        _ensure_header(filename, header)
        file_hnd = open(filename, "a", encoding="utf-8")  # Use append mode
        return file_hnd

    def log(self, utc_time:str, data:tuple):
        """
        Utc_time: str.

        Expected incoming like: 2025-09-16 14:18:00.937022
        utc_time is the UTC datetime recorded

        Only initialize file handler when first data comes in. Therefore there is a side-effect
        on self.fd, self.fd_date which keep tracks of current log file daily
        """
        # TODO check if new data's date (assumed UTC) has changed
        now_date = utc_time.split(" ")[0]

        if not hasattr(self, "fd"):
            self.fd = self._new_file_handler(now_date)
            self.fd_date = now_date
        if self.fd_date != now_date:
            self.close()
            # possibly new month, zip:
            get_year_month = lambda x: (x[0:4], x[5:7])
            if get_year_month(self.fd_date) != get_year_month(now_date):
                # zip last month
                yr,mn = get_year_month(self.fd_date)
                last_month_dir = DATA_PATH / self.symbol / f"{yr}_{mn}"
                zip_fn = last_month_dir.parent / f"{self.symbol}_{self.tf}_{mn}{yr}.zip"
                _zip_old_logs(last_month_dir, zip_fn)

            self.fd = self._new_file_handler(now_date)
            self.fd_date = now_date

        self.fd.write(",".join(map(str, data)) + "\n")
        if datetime.now() >= self.nxt_flush_time:
            self._flush()

    def _flush(self):
        """Flush data to disk and set next flush time."""
        if not hasattr(self, "fd"): return
        self.fd.flush()
        self.nxt_flush_time = datetime.now() + timedelta(minutes=randint(3,7))

    def close(self):
        """Close the file handle safely."""
        if not hasattr(self, "fd"): return
        self.fd.flush()
        self.fd.close()

    def __del__(self):
        """Destructor to ensure file is closed."""
        self.close()

class DataValidator:
    """
    TODO it was found the TF data could be invalid from MT4 server,.

    for example, ohlc are all same value continually, very unlikely
    1. so strange, library WIFI, the data is invalid...
    """

    INVALID_COUNT_THRESHOLD = 30
    def __init__(self, proxy):
        """Proxy: ZMQ_MT4."""
        self._proxy = proxy
        self._invalid_count = defaultdict(int)

    def is_ohlc_same(self, symbol, ohlc):
        is_same = (ohlc["open"] == ohlc["high"] == ohlc["low"] == ohlc["close"])
        cnt = self._invalid_count
        cnt[symbol] = cnt[symbol] + 1 if is_same else 0
        if cnt[symbol] > self.INVALID_COUNT_THRESHOLD:
            print(f"Warning: {symbol} has {self._invalid_count[symbol]}"
                    "consecutive invalid OHLC data.")
            self._proxy.unsubscribe(symbol)

    def is_tick_stale(self):
        """
        Check if tick data is stale (too old).

        The server side has no timestamp in data... should check?
        """
