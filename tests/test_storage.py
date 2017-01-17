import os
import shutil
import tempfile
import unittest

from polymr.record import Record
import polymr.storage


class TestLevelDBBackend(unittest.TestCase):

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
            "leveldb://localhost"+self.workdir)
        return self.db

    def test_get_set_freqs(self):
        db = self._get_db()
        x = {"abc": 3, "bcd": 2}
        db.save_freqs(x)
        y = db.get_freqs()
        self.assertEqual(x, y)

    def test_get_set_rowcount(self):
        db = self._get_db()
        x = 222
        db.save_rowcount(x)
        y = db.get_rowcount()
        self.assertEqual(x, y)

    def test_get_set_token(self):
        db = self._get_db()
        tok = "abc"
        records_simple = [1,2,3]
        db.save_token(tok, records_simple, False)
        rng = db.get_token(tok)
        self.assertEqual(records_simple, rng)
        tok = "bcd"
        records_cmpct = [1,2,[3,6],7]
        db.save_token(tok, records_cmpct, True)
        rng = db.get_token(tok)
        self.assertNotEqual(records_cmpct, rng)
        self.assertEqual(rng, list(range(1,8)))

    def test_get_set_del_tokprefix(self):
        db = self._get_db()
        rng = [1,2,3,4,5,6]
        db.save_tokprefix([ ("abc", 1, rng[3:]),
                            ("abc", 2, rng[:3]) ])
        keys, rngs = db.get_tokprefix("abc")
        self.assertEqual(sorted(rngs[0]+rngs[1]), rng)
        cnt = db.delete_tokprefix("abc")
        self.assertEqual(cnt, 2)
        with self.assertRaises(KeyError):
            db.feature_db.Get("abc_0".encode())

    def test_get_set_toklist(self):
        db = self._get_db()
        alltoks = ["abc", "bcd", "cde"]
        db.save_toklist(alltoks)
        self.assertEqual(alltoks, db.get_toklist())

    def test_get_set_records(self):
        db = self._get_db()
        r1 = Record(["abcde", "foo"], "1")
        r2 = Record(["qwert", "bar"], "2")
        cnt = db.save_records(enumerate((r1, r2)))
        self.assertEqual(cnt, 2)
        r1_db, r2_db = list(db.get_records([0, 1]))
        self.assertEqual(r1.fields, r1_db.fields)
        self.assertEqual(r1.pk, r1_db.pk)
        self.assertEqual(r2.fields, r2_db.fields)
        self.assertEqual(r2.pk, r2_db.pk)
        
