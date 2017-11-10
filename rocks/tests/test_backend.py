import os
import sys
import shutil
import tempfile
import unittest

from polymr.record import Record
import polymr.storage
import polymr_rocksdb

here = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(here, "..", "..", "tests"))

from test_storage import TestLevelDBBackend

polymr_rocksdb  # pyflakes


class TestRocksDBBackend(TestLevelDBBackend):

    def _get_db(self, new=False):
        if self.db and not new:
            return self.db
        if self.db:
            self.db.close()
        self.db = polymr.storage.parse_url(
            "rocksdb://localhost"+self.workdir)
        return self.db

if __name__ == '__main__':
    unittest.main()
