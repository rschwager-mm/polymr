import os
import sys
import unittest
from unittest import skipIf

from polymr.record import Record
import polymr_postgres

here = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(here, "..", "..", "tests"))

from test_storage import TestLevelDBBackend


ENVVAR = "POLYMR_POSTGRES_URL"
URL = os.environ.get(ENVVAR, False)
should_skip_test = not bool(URL)


class TestPostgresBackend(TestLevelDBBackend):
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

    def test_get_set_increment_rowcount(self):
        db = self._get_db()
        r1 = Record(["abcde", "foo"], "1", ['dogsays'])
        r2 = Record(["qwert", "bar"], "2", ['barque'])
        self.assertEqual(db.get_rowcount(), 0)
        db.save_records(enumerate((r1, r2)))
        self.assertEqual(db.get_rowcount(), 2)
        

for methname in dir(TestPostgresBackend):
    if not methname.startswith("test_"):
        continue
    meth = getattr(TestPostgresBackend, methname)
    setattr(TestPostgresBackend, 
            methname,
            skipIf(should_skip_test, ENVVAR+" not defined")(meth))

        
