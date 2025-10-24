import unittest
import csv
import lzma
from pathlib import Path

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

