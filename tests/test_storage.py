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

    def test_get_set_featurizer_name(self):
        db = self._get_db()
        self.assertEqual(db.get_featurizer_name(), "default")
        n = "testytest"
        db.save_featurizer_name(n)
        self.assertEqual(db.get_featurizer_name(), n)

    def test_get_set_update_freqs(self):
        db = self._get_db()
        x = {b"abc": 3, b"bcd": 2}
        db.save_freqs(x)
        y = db.get_freqs()
        self.assertEqual(x, dict(y))
        new_x = {b"abc": 5, b"bcd": 10}
        db.update_freqs(new_x.items())
        y = db.get_freqs()
        self.assertEqual(new_x, dict(y))
        
    def test_get_set_increment_rowcount(self):
        db = self._get_db()
        x = 222
        db.save_rowcount(x)
        y = db.get_rowcount()
        self.assertEqual(x, y)
        inc = 5
        db.increment_rowcount(inc)
        y = db.get_rowcount()
        self.assertEqual(x+inc, y)

    def test_get_set_update_droprec_token(self):
        db = self._get_db()
        tok = b"abc"
        records_simple = [1,2,3]
        db.save_token(tok, records_simple)
        rng = db.get_token(tok)
        self.assertEqual(records_simple, list(rng))
        add = [4]
        db.update_token(tok, add)
        rng = db.get_token(tok)
        self.assertEqual(list(set(records_simple+add)), list(rng))
        db.drop_records_from_token(tok, add)
        rng = db.get_token(tok)
        self.assertEqual([1,2,3], list(rng))

    def test_get_set_del_records(self):
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
        db.delete_record(0)
        with self.assertRaises(KeyError):
            db.get_record(0)
        db.get_record(1)
