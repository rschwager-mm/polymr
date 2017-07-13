import os
from array import array
from itertools import count as counter
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

    def save_token(self, name, record_ids):
        self.feature_db.put(name, array("L", record_ids).tobytes())

    def save_tokens(self, names_ids, chunk_size=5000):
        chunks = partition_all(chunk_size, names_ids)
        for chunk in chunks:
            batch = rocksdb.WriteBatch()
            for name, record_ids in chunk:
                batch.put(name, array("L", record_ids).tobytes())
            self.feature_db.write(batch)

    def _load_record_blob(self, idx):
        blob = self.record_db.get(array("L", (idx,)).tobytes())
        if blob is None:
            raise KeyError
        return blob

    def get_records(self, idxs, chunk_size=1000):
        keys = iter(array("L", (idx,)).tobytes() for idx in idxs)
        chunks = partition_all(chunk_size, keys)
        for chunk in chunks:
            chunk = list(chunk)
            vals = self.record_db.multi_get(chunk)
            for key in chunk:
                blob = vals[key]
                if blob is None:
                    raise KeyError
                yield self._get_record(blob)

    def save_record(self, rec, idx=None, save_rowcount=True):
        idx = self.get_rowcount() + 1 if idx is None else idx
        self.record_db.put(array("L", (idx,)).tobytes(), dumps(rec))
        if save_rowcount is True:
            self.save_rowcount(idx)
        return idx

    def save_records(self, idx_recs, record_db=None, chunk_size=5000):
        chunks = partition_all(chunk_size, idx_recs)
        cnt = counter()
        for chunk in chunks:
            batch = rocksdb.WriteBatch()
            for idx, rec in chunk:
                batch.put(array("L", (idx,)).tobytes(), dumps(rec))
                next(cnt)
            self.record_db.write(batch)
        return next(cnt)

    def delete_record(self, idx):
        self.record_db.delete(array("L", (idx,)).tobytes())


polymr.storage.backends['rocksdb'] = RocksDBBackend
