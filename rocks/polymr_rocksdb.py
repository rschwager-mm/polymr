import os
from itertools import counter
from collections import defaultdict

import rocksdb
import polymr.storage
from polymr.storage import loads
from polymr.storage import dumps
from polymr.storage import LevelDBBackend
from toolz import partition_all


class RocksDBBackend(LevelDBBackend):
    def __init__(self, path=None,
                 create_if_missing=True,
                 featurizer_name=None,
                 feature_db=None,
                 record_db=None,
                 read_only=False):

        self._freqs = None
        if feature_db is not None or record_db is not None:
            self.feature_db = feature_db
            self.record_db = record_db
            self.path = None
            return

        self.path = path
        if create_if_missing and not os.path.exists(path):
            os.mkdir(path)

        self.feature_db = rocksdb.DB(
            os.path.join(path, "features"),
            rocksdb.Options(create_if_missing=create_if_missing),
            read_only=read_only)
        self.record_db = rocksdb.DB(
            os.path.join(path, "records"),
            rocksdb.Options(create_if_missing=create_if_missing),
            read_only=read_only)
        self.featurizer_name = featurizer_name
        if not self.featurizer_name:
            try:
                name = self.get_featurizer_name()
            except:
                name = 'default'
            self.featurizer_name = name
        self._check_dbstats()

    @classmethod
    def from_urlparsed(cls, parsed, featurizer_name=None, read_only=False):
        return cls(parsed.path, featurizer_name=featurizer_name,
                   read_only=read_only)

    def get_freqs(self):
        s = self.feature_db.get(b"Freqs")
        if s is None:
            raise KeyError
        return defaultdict(int, loads(s))

    def save_freqs(self, freqs_dict):
        self.feature_db.put(b"Freqs", dumps(freqs_dict))

    def get_rowcount(self):
        blob = self.record_db.get(b"Rowcount")
        if blob is None:
            raise KeyError
        return loads(blob)

    def save_rowcount(self, cnt):
        self.record_db.put(b"Rowcount", dumps(cnt))

    def _load_token_blob(self, name):
        blob = self.feature_db.get(name)
        if blob is None:
            raise KeyError
        return blob

    def save_token(self, name, record_ids, compacted):
        self.feature_db.put(
            name,
            dumps({b"idxs": record_ids, b"compacted": compacted})
        )

    def save_tokens(self, names_ids_compacteds, chunk_size=5000):
        chunks = partition_all(chunk_size, names_ids_compacteds)
        for chunk in chunks:
            batch = rocksdb.WriteBatch()
            for name, record_ids, compacted in chunk:
                batch.put(
                    name,
                    dumps({b"idxs": record_ids, b"compacted": compacted})
                )
            self.feature_db.write(batch)

    def _load_record_blob(self, idx):
        blob = self.record_db.get(str(idx).encode())
        if blob is None:
            raise KeyError
        return blob

    def get_records(self, idxs):
        keys = list(map(str.encode, map(str, idxs)))
        vals = self.record_db.multi_get(keys)
        for key in keys:
            blob = vals[key]
            if blob is None:
                raise KeyError
            yield self._get_record(blob)

    def save_record(self, rec, idx=None, save_rowcount=True):
        idx = self.get_rowcount() + 1 if idx is None else idx
        self.record_db.put(str(idx).encode(),
                           dumps(rec))
        if save_rowcount is True:
            self.save_rowcount(idx)
        return idx

    def save_records(self, idx_recs, record_db=None, chunk_size=5000):
        chunks = partition_all(chunk_size, idx_recs)
        cnt = counter()
        for chunk in chunks:
            batch = rocksdb.WriteBatch()
            for idx, rec in chunk:
                batch.put(
                    str(idx).encode(),
                    dumps(rec)
                )
                next(cnt)
            self.record_db.write(batch)
        return next(cnt)

    def delete_record(self, idx):
        self.record_db.delete(str(idx).encode())


polymr.storage.backend['rocksdb'] = RocksDBBackend
