import operator
from collections import defaultdict
from itertools import chain
from itertools import repeat
from urllib.parse import urlunparse

from toolz import partition_all
import postgresql

import polymr.storage
from polymr.record import Record
from polymr.storage import loads
from polymr.storage import dumps
from polymr.storage import AbstractBackend

snd = operator.itemgetter(1)
cat = chain.from_iterable


class PostgresBackend(AbstractBackend):
    def __init__(self, url_or_connection=None, create_if_missing=True,
                 featurizer_name=None):
        if type(url_or_connection) is str:
            url = url_or_connection
            self._conn = postgresql.open(url)
        else:
            self._conn = url_or_connection
        if create_if_missing is True and not self.exists():
            self.create()
        self.featurizer_name = featurizer_name
        if not self.featurizer_name:
            try:
                name = self.get_featurizer_name()
            except:
                name = 'default'
            self.featurizer_name = name
        self._check_dbstats()

    def exists(self):
        try:
            self._conn.query.first("SELECT value FROM polymr_settings")
        except:
            return False
        return True

    def create(self):
        self._create_settings()
        self._create_records()
        self._create_features()
        self._create_feature_record_map()

    def _create_settings(self):
        self._conn.execute(
            'CREATE TABLE IF NOT EXISTS polymr_settings ('
            ' name varchar(40) PRIMARY KEY,'
            ' value varchar(256),'
            ' CONSTRAINT polymr_settings_uniq_name UNIQUE(name)'
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

    def _create_features(self):
        self._conn.execute(
            'CREATE TABLE IF NOT EXISTS polymr_features ('
            ' id serial PRIMARY KEY,'
            ' tok bytea,'
            ' freq integer,'
            ' CONSTRAINT polymr_feature_uniq_tok UNIQUE(tok)'
            ');'
        )

    def _create_feature_record_map(self, index=True):
        self._conn.execute(
            'CREATE TABLE IF NOT EXISTS polymr_feature_record_map ('
            ' id_tok integer,'
            ' id_rec integer'
            ');'
        )
        if index is True:
            self._create_feature_record_map_index()

    def _create_feature_record_map_index(self):
        self._conn.execute(
            'CREATE INDEX IF NOT EXISTS polymr_feature_record_map_idx'
            ' ON polymr_feature_record_map USING btree (id_tok);'
        )

    def destroy(self):
        self._conn.execute('DROP TABLE polymr_settings')
        self._conn.execute('DROP TABLE polymr_records')
        self._conn.execute('DROP TABLE polymr_features')
        self._conn.execute('DROP TABLE polymr_feature_record_map')

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
        if not self._has_freqs():
            self.save_freqs({})
        if not self.get_rowcount():
            self.save_rowcount(0)
        try:
            self.get_featurizer_name()
        except KeyError:
            self.save_featurizer_name('default')

    @classmethod
    def from_urlparsed(cls, parsed, featurizer_name=None):
        return cls(urlunparse(parsed), featurizer_name=featurizer_name)

    def close(self):
        self._conn.close()

    def find_least_frequent_tokens(self, toks, r, k=None):
        stmt = self._conn.prepare("SELECT tok, freq FROM polymr_features"
                                  " WHERE tok = $1")
        toks_freqs = sorted(filter(None, map(stmt.first, toks)),
                            key=snd)
        ret = []
        total = 0
        for i, (tok, freq) in enumerate(toks_freqs):
            if total + freq > r:
                break
            total += freq
            ret.append(tok)
            if k and i >= k:
                break
        return ret

    def _has_freqs(self):
        cnt = self._conn.query.first("SELECT COUNT(*) FROM polymr_features")
        return cnt > 1

    def get_freqs(self):
        stmt = self._conn.prepare('SELECT tok, freq FROM polymr_features')
        return defaultdict(int, cat(stmt.chunks()))

    def update_freqs(self, toks_cnts):
        stmt = self._conn.prepare(
            "INSERT INTO polymr_features VALUES (DEFAULT, $1, $2)"
            " ON CONFLICT (tok) DO UPDATE SET freq = EXCLUDED.freq"
        )
        chunks = partition_all(1000, toks_cnts)
        with self._conn.xact():
            stmt.load_chunks(chunks)

    def save_freqs(self, freqs_dict):
        return self.update_freqs(freqs_dict.items())

    def get_rowcount(self):
        return self._conn.query.first(
            "SELECT reltuples FROM pg_class"
            " WHERE oid = 'polymr_records'::regclass")

    def increment_rowcount(self, cnt):
        pass

    def save_rowcount(self, cnt):
        pass

    def get_token(self, name):
        stmt = self._conn.prepare(
            'SELECT b.id_rec FROM polymr_features a'
            ' LEFT JOIN polymr_feature_record_map b ON a.id = b.id_tok'
            ' WHERE a.tok = $1'
        )
        return list(cat(cat(stmt.chunks(name))))

    def update_token(self, name, record_ids):
        self.save_token(name, record_ids, False)

    def drop_records_from_token(self, name, bad_record_ids):
        delete = self._conn.prepare(
            "DELETE FROM polymr_feature_record_map"
            " WHERE id_tok in (SELECT id FROM polymr_features WHERE tok = $1)"
            " AND id_rec = $2"
        )
        with self._conn.xact():
            delete.load_rows(zip(repeat(name), bad_record_ids))

    def save_token(self, name, record_ids, compacted=False):
        if compacted is False:
            record_id_len = len(record_ids)
        else:
            record_id_len = sum(1 if type(i) is int else i[1] - i[0] + 1
                                for i in record_ids)
        tok_id = self._conn.prepare(
            "INSERT INTO polymr_features AS a VALUES (DEFAULT, $1, $2)"
            " ON CONFLICT (tok) DO UPDATE SET freq = a.freq + EXCLUDED.freq"
            " RETURNING id"
        ).first(name, record_id_len)
        stmt = self._conn.prepare(
            'INSERT INTO polymr_feature_record_map VALUES ($1, $2)'
        )
        with self._conn.xact():
            if compacted is False:
                stmt.load_rows(zip(repeat(tok_id), record_ids))
            else:
                for record_id in record_ids:
                    if type(record_id) is list:
                        ids = range(record_id[0], record_id[1]+1)
                        stmt.load_rows(zip(repeat(tok_id), ids))
                    else:
                        stmt(tok_id, record_id)

    def save_tokens(self, names_ids):
        stmt = self._conn.prepare("COPY polymr_feature_record_map FROM STDIN")
        ids = self._conn.query.chunks("SELECT tok, id FROM polymr_features")
        tok_cache = dict(cat(ids))

        def _rows():
            for name, record_ids in names_ids:
                for record_id in record_ids:
                    tok_id = tok_cache[name]
                    if type(record_id) is list:
                        for i in range(record_id[0], record_id[1]+1):
                            yield '{}\t{}\n'.format(tok_id, i).encode()
                    else:
                        yield '{}\t{}\n'.format(tok_id, record_id).encode()

        with self._conn.xact():
            stmt.load_rows(_rows())

    def _prepare_record_select(self):
        return self._conn.prepare(
            'SELECT fields, pk, data FROM polymr_records WHERE id = $1'
        )

    def _get_record(self, idx, stmt):
        packed = stmt.first(idx)
        if packed is None:
            raise KeyError
        return Record(list(map(bytes.decode, loads(packed[0]))),
                      packed[1],
                      loads(packed[2]))

    def get_record(self, idx):
        stmt = self._prepare_record_select()
        return self._get_record(idx, stmt)

    def get_records(self, idxs):
        stmt = self._prepare_record_select()
        for idx in idxs:
            yield self._get_record(idx, stmt)

    def save_record(self, rec, idx=None, save_rowcount=True):
        stmt = self._conn.prepare(
            'INSERT INTO polymr_records VALUES (DEFAULT, $1, $2, $3)'
            ' RETURNING id'
        )
        with self._conn.xact():
            idx = stmt.first(dumps(rec.fields), str(rec.pk), dumps(rec.data))
        return idx

    def save_records(self, idx_recs, record_db=None, chunk_size=5000):
        stmt = self._conn.prepare(
            'INSERT INTO polymr_records VALUES (DEFAULT, $1, $2, $3)'
        )
        rows = iter((dumps(rec.fields), str(rec.pk), dumps(rec.data))
                    for rec in map(snd, idx_recs))
        chunks = partition_all(chunk_size, rows)
        with self._conn.xact():
            stmt.load_chunks(chunks)

    def delete_record(self, idx):
        self._conn.prepare('DELETE from polymr_records WHERE id = $1')(idx)


polymr.storage.backends['postgres'] = PostgresBackend
polymr.storage.backends['pq'] = PostgresBackend
