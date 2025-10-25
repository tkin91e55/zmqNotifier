# ruff: noqa
import unittest
from pathlib import Path

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
        from zmqNotifier.csv_logger import _zip_old_logs

        # Create a test directory with dummy CSV files
        test_dir = self.TEST_DATA_PATH / 'test_zip'
        test_dir.mkdir(parents=True, exist_ok=True)

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
