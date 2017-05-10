import os
import shutil
import tempfile
import unittest

from polymr.record import Record
import polymr.storage
import polymr_rocksdb


polymr_rocksdb  # pyflakes


class TestRocksDBBackend(unittest.TestCase):

    def setUp(self):
        self.workdir = tempfile.mkdtemp(suffix="polymrtest")
        self.db = None

    def tearDown(self):
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def _get_db(self, new=False):
        if self.db and not new:
            return self.db
        if self.db:
            self.db.close()
        self.db = polymr.storage.parse_url(
            "rocksdb://localhost"+self.workdir)
        return self.db

    def test_get_set_freqs(self):
        db = self._get_db()
        x = {b"abc": 3, b"bcd": 2}
        db.save_freqs(x)
        y = db.get_freqs()
        self.assertEqual(x, dict(y))

    def test_get_set_rowcount(self):
        db = self._get_db()
        x = 222
        db.save_rowcount(x)
        y = db.get_rowcount()
        self.assertEqual(x, y)

    def test_get_set_token(self):
        db = self._get_db()
        tok = b"abc"
        records_simple = [1,2,3]
        db.save_token(tok, records_simple, False)
        rng = db.get_token(tok)
        self.assertEqual(records_simple, rng)
        tok = b"bcd"
        records_cmpct = [1,2,[3,6],7]
        db.save_token(tok, records_cmpct, True)
        rng = db.get_token(tok)
        self.assertNotEqual(records_cmpct, rng)
        self.assertEqual(rng, list(range(1,8)))

    def test_get_set_records(self):
        db = self._get_db()
        r1 = Record(["abcde", "foo"], "1", ['dogsays'])
        r2 = Record(["qwert", "bar"], "2", ['barque'])
        cnt = db.save_records(enumerate((r1, r2)))
        self.assertEqual(cnt, 2)
        r1_db, r2_db = list(db.get_records([0, 1]))
        self.assertEqual(r1.fields, r1_db.fields)
        self.assertEqual(r1.pk, r1_db.pk)
        self.assertEqual(r2.fields, r2_db.fields)
        self.assertEqual(r2.pk, r2_db.pk)
