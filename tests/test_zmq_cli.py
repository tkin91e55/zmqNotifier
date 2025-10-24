# ruff: noqa
import csv
import lzma
import unittest
from pathlib import Path

# run: python -m unittest zmqNotifier.zmq_cli_test

@unittest.skip("skipped")
class TestGZip(unittest.TestCase):

    def test_gzip_compression(self):
        fn = 'zmqNotifier/data/test_gzip.csv.xz'
        if Path(fn).exists():
            Path(fn).unlink()

        row_one = b'a;b;c;d'
        row_two = b'1;2;3;4'
        with lzma.open(fn, 'ab') as lz:
            lz.write(row_one)
        with lzma.open(fn, 'ab') as lz:
            lz.write(row_two)

        # The above can write streams into .xz file and reopenable, but
        # no \n between rows

        # with lzma.open(fn, 'rt') as lz:
        #     lines = lz.readlines()
        #     self.assertEqual(lines[0].strip(), row_one)
        #     self.assertEqual(lines[1].strip(), row_two)

    @unittest.skip("demonstration, don't want generated files")
    def test_lzma_compression(self):
        # Example usage
        data = [
            ['Name', 'Age', 'City'],
            ['Alice', 30, 'New York'],
            ['Bob', 25, 'Los Angeles'],
            ['Charlie', 35, 'Chicago'],
        ] * 50
        fn = 'zmqNotifier/data/test_lz-{0}.csv.xz'

        # version 1: 300 bytes
        for row in data:
            with lzma.open(fn.format(1), 'at', newline='', encoding='utf-8') as lzma_file:
                writer = csv.writer(lzma_file)
                writer.writerow(row)

        # version 2: 132 bytes
        with lzma.open(fn.format(2), 'at', newline='', encoding='utf-8') as lzma_file:
            writer = csv.writer(lzma_file)
            for row in data:
                writer.writerow(row)

        # version 3: 132 bytes, same to 2
        # with lzma.open(fn.format(3), 'wb', encoding='ascii') as lzma_file: # wrong
        with lzma.open(fn.format(3), 'wt') as lzma_file:
            writer = csv.writer(lzma_file)
            for row in data:
                writer.writerow(row)

        # Therefore do logorate, rather writing streams of bytes

    def test_csv2lzma(self):
        fn = 'zmqNotifier/data/test_lz-2.csv'
        if not Path(fn).exists():
            print('run the test_lzma_compression() first')
            return
        import shutil
        with open(fn, 'rb') as f_in:
            with lzma.open('zmqNotifier/data/test.csv.xz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

class TestZMQMT4(unittest.TestCase):

    @classmethod
    def setUpclass(cls):
        from zmq_cli import ZMQ_MT4
        cls.subscriber = ZMQ_MT4()
        import pickle
        with open('zmqNotifier/data/zmq_market_db.pickle','rb') as pf:
            cls.db = pickle.load(pf)

    def test_validate_onebaronetick(self):
        pass

from unittest.mock import patch

from zmqNotifier.csv_logger import DataLogger


@patch('zmqNotifier.csv_logger.DATA_PATH', Path('./zmqNotifier/test_data'))
class TestDataLogger(unittest.TestCase):
    """
    Test basic functions of DataLogger, API is supposed to be used like:

    logger = DataLogger(proxy, symbol, tf)
    logger.log(utc_dt,data) # repeat... and knows to log rotate zips
    logger.close() # at the end of the program
    """

    TEST_DATA_PATH = Path('./zmqNotifier/test_data')

    # Mock proxy object for DataLogger
    mock_proxy = type('MockProxy', (), {})()
    test_symbol = 'BTCUSD'

    @classmethod
    def setUpClass(cls):
        if not cls.TEST_DATA_PATH.exists():
            cls.TEST_DATA_PATH.mkdir(parents=True)

    # @classmethod
    # def tearDownClass(cls):
    #     import shutil
    #     if cls.TEST_DATA_PATH.exists():
    #         shutil.rmtree(cls.TEST_DATA_PATH)

    def test_datalogger_initial_state(self):
        """Test DataLogger initialization for tick data."""
        logger = DataLogger(self.mock_proxy, self.test_symbol, 'tick')

        # logger close() has no problem, even if no file created
        logger.close()
        logger._flush()

        self.assertFalse(hasattr(logger, 'fd'))
        self.assertFalse(hasattr(logger, 'fd_date'))

    def test_zip_old_logs(self):
        """Test zipping old log files."""
        from .csv_logger import _zip_old_logs

        # Create a test directory with dummy CSV files
        test_dir = self.TEST_DATA_PATH / 'test_zip'
        test_dir.mkdir(parents=True)

        # Create dummy CSV files
        for i in range(3):
            with open(test_dir / f'test_file_{i}.csv', 'w', encoding='utf-8') as f:
                f.write('header1,header2\n')
                f.write('data1,data2\n')

        output_zip = self.TEST_DATA_PATH / 'test_zip_output.zip'
        if output_zip.exists():
            output_zip.unlink()

        _zip_old_logs(test_dir, output_zip)

        # Check that the zip file is created
        self.assertTrue(output_zip.exists())

        # Check that the original CSV files are deleted
        for i in range(3):
            self.assertFalse((test_dir / f'test_file_{i}.csv').exists())

    def test_new_file_hanlder(self):
        """Test creating a new file handler."""
        L = DataLogger(self.mock_proxy, self.test_symbol, 'tick')
        test_date = '2025-10-10'
        fd = L._new_file_handler(test_date)

        # Check that file descriptor is created
        # self.assertTrue(hasattr(L, 'fd'))
        # self.assertTrue(hasattr(L, 'fd_date'))
        # self.assertEqual(L.fd_date, test_date)

        # Check that the file path follows expected structure
        expected_parent = self.TEST_DATA_PATH / L.symbol / test_date[:7].replace('-', '_')
        expected_filename = expected_parent / f"{L.symbol}_{L.tf}_{test_date}.csv"

        self.assertTrue(expected_parent.exists())
        self.assertTrue(expected_filename.exists())

    # def test_datalogger_initialization_ohlc(self):
    #     """Test DataLogger initialization for OHLC bar data."""
    #     from csv_logger import DataLogger

    #     logger = DataLogger(self.mock_proxy, self.test_symbol, 'M1')

    #     # Check that file descriptor is created
    #     self.assertIsNotNone(logger.fd)
    #     # Check symbol and timeframe are set correctly
    #     self.assertEqual(logger._symbol, self.test_symbol)
    #     self.assertEqual(logger._tf, 'M1')
    #     logger.close()

    # def test_write_tick_data(self):
    #     """Test writing tick data to CSV."""
    #     from csv_logger import DataLogger

    #     logger = DataLogger(self.mock_proxy, self.test_symbol, 'tick')

    #     # Sample tick data: datetime, bid, ask
    #     tick_data = {
    #         'datetime': '2025-10-10 12:00:00',
    #         'bid': 1.05123,
    #         'ask': 1.05125
    #     }

    #     # This should not raise an exception when implemented
    #     logger.write(tick_data)
    #     logger.flush()
    #     logger.close()

    # def test_write_bar_data(self):
    #     """Test writing OHLC bar data to CSV."""
    #     from csv_logger import DataLogger

    #     logger = DataLogger(self.mock_proxy, self.test_symbol, 'M1')

    #     # Sample OHLC data: datetime, open, high, low, close, volume
    #     bar_data = {
    #         'datetime': '2025-10-10 12:00:00',
    #         'open': 1.05120,
    #         'high': 1.05130,
    #         'low': 1.05115,
    #         'close': 1.05125,
    #         'volume': 100
    #     }

    #     # This should not raise an exception when implemented
    #     logger.write(bar_data)
    #     logger.flush()
    #     logger.close()

    # def test_file_structure_creation(self):
    #     """Test that proper directory structure is created."""
    #     from csv_logger import DataLogger
    #     from datetime import datetime

    #     logger = DataLogger(self.mock_proxy, self.test_symbol, 'M1')

    #     # Check that the file path follows expected structure
    #     today = datetime.today()
    #     expected_parent = Path('./data') / self.test_symbol / f"{today.year:04d}_{today.month:02d}"
    #     expected_filename = expected_parent / f"{self.test_symbol}_M1_{today.strftime('%Y-%m-%d')}.csv"

    #     # The directory should exist
    #     self.assertTrue(expected_parent.exists())
    #     logger.close()

    # def test_header_creation_tick(self):
    #     """Test that proper CSV headers are created for tick data."""
    #     from csv_logger import DataLogger

    #     logger = DataLogger(self.mock_proxy, self.test_symbol, 'tick')

    #     # Read the first line to check header
    #     logger.fd.seek(0)
    #     first_line = logger.fd.readline().strip()
    #     expected_header = "datetime,bid,ask"

    #     self.assertEqual(first_line, expected_header)
    #     logger.close()

    # def test_header_creation_ohlc(self):
    #     """Test that proper CSV headers are created for OHLC data."""
    #     from csv_logger import DataLogger

    #     logger = DataLogger(self.mock_proxy, self.test_symbol, 'M1')

    #     # Read the first line to check header
    #     logger.fd.seek(0)
    #     first_line = logger.fd.readline().strip()
    #     expected_header = "datetime,open,high,low,close,volume"

    #     self.assertEqual(first_line, expected_header)
    #     logger.close()

# class TestDataValidator(unittest.TestCase):

#     def setUp(self):
#         """Set up test fixtures before each test method."""
#         # Mock proxy object for DataValidator
#         self.mock_proxy = type('MockProxy', (), {'unsubscribe': lambda x: None})()
#         self.test_symbol = 'EURUSD'

#     def test_datavalidator_initialization(self):
#         """Test DataValidator initialization."""
#         from csv_logger import DataValidator

#         validator = DataValidator(self.mock_proxy)

#         # Check that invalid count dictionary is initialized
#         self.assertEqual(len(validator._invalid_count), 0)
#         self.assertEqual(validator.INVALID_COUNT_THRESHOLD, 30)

#     def test_ohlc_same_values_detection(self):
#         """Test detection of OHLC with same values."""
#         from csv_logger import DataValidator

#         validator = DataValidator(self.mock_proxy)

#         # Test with same OHLC values
#         same_ohlc = {
#             'open': 1.05000,
#             'high': 1.05000,
#             'low': 1.05000,
#             'close': 1.05000
#         }

#         # Should increment invalid count
#         validator.is_ohlc_same(self.test_symbol, same_ohlc)
#         self.assertEqual(validator._invalid_count[self.test_symbol], 1)

#         # Test with different OHLC values
#         valid_ohlc = {
#             'open': 1.05000,
#             'high': 1.05010,
#             'low': 1.04990,
#             'close': 1.05005
#         }

#         # Should reset invalid count
#         validator.is_ohlc_same(self.test_symbol, valid_ohlc)
#         self.assertEqual(validator._invalid_count[self.test_symbol], 0)

#     def test_invalid_count_threshold(self):
#         """Test that unsubscribe is called when threshold is exceeded."""
#         from csv_logger import DataValidator

#         # Mock proxy with unsubscribe tracking
#         unsubscribe_calls = []
#         mock_proxy = type('MockProxy', (), {
#             'unsubscribe': lambda symbol: unsubscribe_calls.append(symbol)
#         })()

#         validator = DataValidator(mock_proxy)

#         same_ohlc = {
#             'open': 1.05000,
#             'high': 1.05000,
#             'low': 1.05000,
#             'close': 1.05000
#         }

#         # Call is_ohlc_same enough times to exceed threshold
#         for i in range(validator.INVALID_COUNT_THRESHOLD + 1):
#             validator.is_ohlc_same(self.test_symbol, same_ohlc)

#         # Should have called unsubscribe
#         self.assertIn(self.test_symbol, unsubscribe_calls)

#     @unittest.skip("requires implementation of is_tick_stale method")
#     def test_tick_stale_detection(self):
#         """Test detection of stale tick data."""
#         # TODO: Implement after is_tick_stale method is completed
#         pass
