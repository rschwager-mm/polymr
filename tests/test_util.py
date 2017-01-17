import unittest

from polymr.util import ngrams
from polymr.util import merge_to_range


class TestUtil(unittest.TestCase):
    def test_ngrams(self):
        self.assertEqual(ngrams("fish", 2, 1), ["fi", "is", "sh"])
        self.assertEqual(ngrams("fish", 3, 1), ["fis", "ish"])
        self.assertEqual(ngrams("fish", 2, 2), ["fi", "sh"])
        self.assertEqual(ngrams("fish", 2, 3), ["fi"])
        self.assertEqual(ngrams("fish", 4, 1), ["fish"])
        self.assertEqual(ngrams("fish", 4, 2), ["fish"])
        self.assertEqual(ngrams("fish", 5, 1), ["fish"])

    def test_merge_to_range(self):
        eq = lambda x, y: self.assertEqual(merge_to_range(x), y)
        eq([[1,2,3]], ([[1,3]], True))
        eq([[1,2,3,4,5]], ([[1,5]], True))
        eq([[1,3,5], [2, 4]], ([[1,5]], True))
        eq([[1,6], [3, 8]], ([1,3,6,8], False))


if __name__ == '__main__':
    unittest.main()
