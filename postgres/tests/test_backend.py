import os
import shutil
import tempfile
import unittest
from unittest import skipIf

from polymr.record import Record
import polymr_postgres

ENVVAR = "POLYMR_POSTGRES_URL"
URL = os.environ.get(ENVVAR, False)
should_skip_test = not bool(URL)


class TestPostgresBackend(unittest.TestCase):
    def setUp(self):
        self.db = polymr_postgres.PostgresBackend(URL)

    def tearDown(self):
        self.db.destroy()
        self.db.close()

    def _get_db(self, new=False):
        if self.db and not new:
            return self.db
        if self.db:
            self.db.close()
        self.db = polymr_postgres.PostgresBackend(URL)
        return self.db

    @skipIf(should_skip_test, ENVVAR+" not defined")
    def test_get_set_freqs(self):
        db = self._get_db()
        x = {b"abc": 3, b"bcd": 2}
        db.save_freqs(x)
        y = db.get_freqs()
        self.assertEqual(x, dict(y))

    @skipIf(should_skip_test, ENVVAR+" not defined")
    def test_get_set_rowcount(self):
        db = self._get_db()
        self.assertEqual(db.get_rowcount(), 0)
        y = db.get_rowcount()
        r1 = Record(["abcde", "foo"], "1", ['dogsays'])
        r2 = Record(["qwert", "bar"], "2", ['barque'])
        db.save_records(enumerate((r1, r2)))
        self.assertEqual(db.get_rowcount(), 2)

    @skipIf(should_skip_test, ENVVAR+" not defined")
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

    @skipIf(should_skip_test, ENVVAR+" not defined")
    def test_get_set_records(self):
        db = self._get_db()
        r1 = Record(["abcde", "foo"], "1", ['dogsays'])
        r2 = Record(["qwert", "bar"], "2", ['barque'])
        db.save_records(enumerate((r1, r2)))
        self.assertEqual(db.get_rowcount(), 2)
        r1_db, r2_db = list(db.get_records([0, 1]))
        self.assertEqual(r1.fields, r1_db.fields)
        self.assertEqual(r1.pk, r1_db.pk)
        self.assertEqual(r2.fields, r2_db.fields)
        self.assertEqual(r2.pk, r2_db.pk)
        
