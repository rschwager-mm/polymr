import random
import unittest

from polymr.record import Record
import polymr_dynamodb


class TestDynamoDBBackend(unittest.TestCase):

    def setUp(self):
        self.db = polymr_dynamodb.DynamoDBBackend(
            "polymr_dynamodb_test_{}".format(random.randint(0, 1<<19)),
            consistent=True, create_if_missing=True)

    def tearDown(self):
        self.db.destroy()

    def test_get_set_rowcount(self):
        db = self.db
        x = 222
        db.save_rowcount(x)
        y = db.get_rowcount()
        self.assertEqual(x, y)

    def test_get_set_token(self):
        db = self.db
        tok = b"abc"
        records_simple = [1,2,3]
        db.save_token(tok, records_simple, False)
        rng = db.get_token(tok)
        self.assertEqual(records_simple, list(rng))
        tok = b"bcd"

    def test_get_set_records(self):
        db = self.db
        r1 = Record(["abcde", "foo"], "1", ['dogsays'])
        r2 = Record(["qwert", "bar"], "2", ['barque'])
        cnt = db.save_records(enumerate((r1, r2)))
        self.assertEqual(cnt, 2)
        r1_db, r2_db = list(db.get_records([0, 1]))
        self.assertEqual(r1.fields, r1_db.fields)
        self.assertEqual(r1.pk, r1_db.pk)
        self.assertEqual(r2.fields, r2_db.fields)
        self.assertEqual(r2.pk, r2_db.pk)
