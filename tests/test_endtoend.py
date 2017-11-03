import os
import shutil
import tempfile
import unittest
from io import StringIO

import polymr.index
import polymr.storage
import polymr.query
import polymr.record
import polymr.score

to_index = StringIO("""01001,MA,DONNA,AGAWAM,WUCHERT,PO BOX 329,9799PNOVAY
01007,MA,BERONE,BELCHERTOWN,BOARDWAY,135 FEDERAL ST,9799JA8CB5
01013,MA,JAMES,CHICOPEE,GIBBONS,5 BURTON ST,9899JBVI6N
01020,MA,LEON,CHICOPEE,NADEAU JR,793 PENDLETON AVE,9799XCPW93
01027,MA,KARA,WESTHAMPTON,SNYDER,18 SOUTH RD,9898OO5MO2
01027,MA,MARY,EASTHAMPTON,STEELE,4 TREEHOUSE CIR,9799QHHOKQ
01030,MA,MELANI,FEEDING HILLS,PICKETT,18 PAUL REVERE DR,989960D48D
01032,MA,JILL,GOSHEN,CARTER,PO BOX 133,9899M4GE2J
01039,MA,PAT,HAYDENVILLE,NEWMAN,4 THE JOG,9799VIXQ81
01040,MA,MARIE,HOLYOKE,KANJAMIE,582 PLEASANT ST,98984OB8OT
""")

sample_query = ["01030","MELANI","PICKETT","18 PAUL REVERE DR"]
sample_pk = "989960D48D"

def custom_extract(fields):
    return polymr.score.features(fields[:-1])


def custom_score(features_a, features_b):
    return polymr.score.hit(features_a, features_b) / 2


class TestEndToEnd(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp(suffix="polymrtest")
        self.db = polymr.storage.parse_url(
            "leveldb://localhost"+self.workdir)
        to_index.seek(0)

    def tearDown(self):
        self.db.close()
        del self.db
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)
        to_index.seek(0)

    def test_end_to_end(self):
        recs = polymr.record.from_csv(
            to_index,
            searched_fields_idxs=[0,2,4,5],
            pk_field_idx=-1,
            include_data=False
        )
        polymr.index.create(recs, 1, 10, self.db)
        index = polymr.query.Index(self.db)
        hit = index.search(sample_query, limit=1)[0]
        self.assertEqual(hit['pk'], sample_pk,
                         ("querying the index with an indexed record should "
                          "return that same record"))

        tpyo = list(sample_query[0])
        tpyo[2], tpyo[3] = tpyo[3], tpyo[2]
        tpyo = "".join(tpyo)
        hit = index.search([tpyo]+sample_query[1:], limit=1)[0]
        self.assertEqual(hit['pk'], sample_pk, "searches should survive typos")

        noncustom_score = hit['score']
        hit = index.search([tpyo]+sample_query[1:], limit=1,
                           score_func=custom_score)[0]
        self.assertEqual(hit['pk'], sample_pk)
        self.assertEqual(hit['score'] * 2, noncustom_score)

        hit = index.search([tpyo]+sample_query[1:], limit=1,
                           extract_func=custom_extract)[0]
        self.assertEqual(hit['pk'], sample_pk)


class TestEndToEndParallel(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp(suffix="polymrtest")
        self.url = "leveldb://localhost"+self.workdir
        to_index.seek(0)

    def tearDown(self):
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)
        to_index.seek(0)

    def test_end_to_end(self):
        recs = polymr.record.from_csv(
            to_index,
            searched_fields_idxs=[0,2,4,5],
            pk_field_idx=-1,
            include_data=False
        )
        db = polymr.storage.parse_url(self.url)
        polymr.index.create(recs, 1, 10, db)
        del db
        index = polymr.query.ParallelIndex(self.url, 2)
        hit = index.search(sample_query, limit=1)[0]
        self.assertEqual(hit['pk'], sample_pk,
                         ("querying the index with an indexed record should "
                          "return that same record"))

        tpyo1 = list(sample_query[0])
        tpyo1[2], tpyo1[3] = tpyo1[3], tpyo1[2]
        tpyo1 = "".join(tpyo1)
        tpyo2 = list(sample_query[0])
        tpyo2[1], tpyo2[2] = tpyo2[2], tpyo2[1]
        tpyo2 = "".join(tpyo2)
        results = index.searchmany([[tpyo1]+sample_query[1:],
                                    [tpyo2]+sample_query[1:]], limit=1)
        noncustom_scores = []
        for result in results:
            hit = result[0]
            noncustom_scores.append(hit['score'])
            self.assertEqual(hit['pk'], sample_pk,
                             "searches should survive typos")

        # custom score function
        results = index.searchmany([[tpyo1]+sample_query[1:],
                                    [tpyo2]+sample_query[1:]],
                                   score_func=custom_score, limit=1)
        for i, result in enumerate(results):
            hit = result[0]
            self.assertEqual(hit['score'] * 2, noncustom_scores[i])
            self.assertEqual(hit['pk'], sample_pk)

        # custom extract function
        results = index.searchmany([[tpyo1]+sample_query[1:],
                                    [tpyo2]+sample_query[1:]],
                                   extract_func=custom_extract, limit=1)
        extract_scores = []
        for result in results:
            hit = result[0]
            extract_scores.append(hit['score'])
            self.assertEqual(hit['pk'], sample_pk)

        # custom extract and custom score functions
        results = index.searchmany([[tpyo1]+sample_query[1:],
                                    [tpyo2]+sample_query[1:]],
                                   extract_func=custom_extract,
                                   score_func=custom_score, limit=1)
        for i, result in enumerate(results):
            hit = result[0]
            self.assertEqual(hit['pk'], sample_pk)
            self.assertEqual(hit['score'] * 2, extract_scores[i])


if __name__ == '__main__':
    unittest.main()
