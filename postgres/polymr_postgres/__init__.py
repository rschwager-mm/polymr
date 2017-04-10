import operator
from collections import defaultdict
from itertools import chain
from urllib.parse import urlunparse

from toolz import partition_all
import postgresql

from polymr.record import Record
from polymr.storage import loads
from polymr.storage import dumps
from polymr.storage import AbstractBackend

snd = operator.itemgetter(1)
cat = chain.from_iterable


class PostgresBackend(AbstractBackend):
    def __init__(self, url_or_connection=None, create_if_missing=True,
                 featurizer_name='default'):
        if type(url_or_connection) is str:
            url = url_or_connection
            self._conn = postgresql.open(url)
        else:
            self._conn = url_or_connection
        if create_if_missing:
            self.create()
        self.featurizer_name = featurizer_name
        if not self.featurizer_name:
            try:
                name = self.get_featurizer_name()
            except:
                name = 'default'
            self.featurizer_name = name
        self._check_dbstats()

    def create(self):
        self._create_settings()
        self._create_records()
        self._create_features()
        self._create_frequencies()

    def _create_settings(self):
        self._conn.execute(
            'CREATE TABLE IF NOT EXISTS polymr_settings ('
            ' name varchar(40) PRIMARY KEY,'
            ' value varchar(256),'
            ' CONSTRAINT con1 UNIQUE(name)'
            ');'
        )

    def _create_records(self):
        self._conn.execute(
            'CREATE TABLE IF NOT EXISTS polymr_records ('
            ' id serial PRIMARY KEY,'
            ' fields bytea,'
            ' pk varchar(128),'
            ' data bytea'
            ');'
        )
        self._conn.execute("ALTER SEQUENCE polymr_records_id_seq MINVALUE 0")
        self._conn.execute("ALTER SEQUENCE polymr_records_id_seq"
                           " RESTART WITH 0")

    def _create_features(self, index=True):
        self._conn.execute(
            'CREATE TABLE IF NOT EXISTS polymr_features ('
            ' tok bytea,'
            ' id integer,'
            ' to_ integer'
            ');'
        )
        if index is True:
            self._create_feature_index()

    def _create_feature_index(self):
        self._conn.execute(
            'CREATE INDEX IF NOT EXISTS polymr_features_tokidx'
            ' ON polymr_features USING btree (tok);'
        )

    def _create_frequencies(self):
        self._conn.execute(
            'CREATE TABLE IF NOT EXISTS polymr_frequencies ('
            ' tok bytea,'
            ' cnt integer'
            ');'
        )

    def destroy(self):
        self._conn.execute('DROP TABLE polymr_settings')
        self._conn.execute('DROP TABLE polymr_records')
        self._conn.execute('DROP TABLE polymr_features')
        self._conn.execute('DROP TABLE polymr_frequencies')

    def get_featurizer_name(self):
        ress = self._conn.query("SELECT value FROM polymr_settings"
                                " WHERE name = 'featurizer'")
        if not ress:
            raise KeyError
        return ress[0]

    def save_featurizer_name(self, name):
        stmt = self._conn.prepare(
            'INSERT INTO polymr_settings VALUES ($1, $2)'
            ' ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name;'
        )
        stmt('featurizer', name)

    def _check_dbstats(self):
        if not self.get_freqs():
            self.save_freqs({})
        if not self.get_rowcount():
            self.save_rowcount(0)
        try:
            self.get_featurizer_name()
        except KeyError:
            self.save_featurizer_name('default')

    @classmethod
    def from_urlparsed(cls, parsed):
        return cls(urlunparse(parsed))

    def close(self):
        self._conn.close()

    def get_freqs(self):
        stmt = self._conn.prepare('SELECT tok, cnt FROM polymr_frequencies')
        return defaultdict(int, cat(stmt.chunks()))

    def save_freqs(self, freqs_dict):
        self._conn.execute('TRUNCATE TABLE polymr_frequencies')
        stmt = self._conn.prepare('INSERT INTO polymr_frequencies'
                                  ' VALUES ($1, $2)')
        chunks = partition_all(1000, freqs_dict.items())
        with self._conn.xact():
            stmt.load_chunks(chunks)

    def get_rowcount(self):
        ress = self._conn.query('SELECT COUNT(*) FROM polymr_records')
        return ress[0][0]

    def save_rowcount(self, cnt):
        pass

    def get_token(self, name):
        stmt = self._conn.prepare(
            'SELECT tok, id, to_ FROM polymr_features WHERE tok = $1'
        )
        idxs = []
        for tok, frm, to in cat(stmt.chunks(name)):
            if to is None:
                idxs.append(frm)
            else:
                idxs.extend(list(range(frm, to+1)))
        return idxs

    def save_token(self, name, record_ids, compacted):
        stmt = self._conn.prepare(
            'INSERT INTO polymr_features VALUES ($1, $2, $3)'
        )
        with self._conn.xact():
            for record_id in record_ids:
                if type(record_id) is list:
                    stmt(name, record_id[0], record_id[1])
                else:
                    stmt(name, record_id, None)

    def get_records(self, idxs):
        stmt = self._conn.prepare(
            'SELECT fields, pk, data FROM polymr_records WHERE id = $1'
        )
        for idx in idxs:
            packed = stmt.first(idx)
            if packed is None:
                raise KeyError
            yield Record(list(map(bytes.decode, loads(packed[0]))),
                         packed[1],
                         loads(packed[2]))

    def save_records(self, idx_recs, record_db=None):
        stmt = self._conn.prepare(
            'INSERT INTO polymr_records VALUES (DEFAULT, $1, $2, $3)'
        )
        rows = iter((dumps(rec.fields), str(rec.pk), dumps(rec.data))
                    for rec in map(snd, idx_recs))
        chunks = partition_all(1000, rows)
        with self._conn.xact():
            stmt.load_chunks(chunks)

    def delete_record(self, idx):
        self._conn.prepare('DELETE from polymr_records WHERE id = $1')(idx)
