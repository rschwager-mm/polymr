import os
import unittest
from unittest import skipIf
from io import StringIO

import polymr.index
import polymr.storage
import polymr.query
import polymr.record
import polymr_postgres

ENVVAR = "POLYMR_POSTGRES_URL"
URL = os.environ.get(ENVVAR, False)
should_skip_test = not bool(URL)

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


class TestEndToEndWithPostgres(unittest.TestCase):
    def setUp(self):
        self.db = polymr_postgres.PostgresBackend(URL)

    def tearDown(self):
        self.db.destroy()
        self.db.close()

    @skipIf(should_skip_test, ENVVAR+" not defined")
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
        self.assertEqual(hit['pk'], sample_pk,"searches should survive typos")
        
        
if __name__ == '__main__':
    unittest.main()
