from array import array
from collections import defaultdict

import redis
import polymr.storage
from polymr.storage import dumps
from polymr.storage import LevelDBBackend
from toolz import partition_all
from toolz import valmap


class RedisBackend(LevelDBBackend):
    def __init__(self, host='localhost', port=6379, db=0,
                 featurizer_name=None, new=False):
        self._freqs = None
        self.featurizer_name = featurizer_name
        self.r = redis.StrictRedis(host=host, port=port, db=db)
        if new is True:
            self.r.flushdb()
        if not self.featurizer_name:
            try:
                self.featurizer_name = self.get_featurizer_name()
            except OSError:
                self.featurizer_name = 'default'
        self._check_dbstats()

    @classmethod
    def from_urlparsed(cls, parsed, featurizer_name=None, read_only=None):
        path = parsed.path.strip("/") or 0
        return cls(host=parsed.host, port=parsed.port, db=path,
                   featurizer_name=featurizer_name)

    def close(self):
        pass

    def get_featurizer_name(self):
        ret = self.r.get(b'featurizer')
        if ret is None:
            raise OSError
        return ret.decode()

    def save_featurizer_name(self, name):
        self.r.set(b'featurizer', name)

    def get_freqs(self):
        return defaultdict(int, valmap(int, self.r.hgetall(b'freqs')))

    def save_freqs(self, freqs_dict):
        for k, v in freqs_dict.items():
            self.r.hset(b"freqs", k, v)

    def get_rowcount(self):
        ret = self.r.get(b'rowcount')
        if ret is None:
            return 0
        return int(ret)

    def save_rowcount(self, cnt):
        self.r.set(b'rowcount', cnt)

    def increment_rowcount(self, cnt):
        self.r.incr(b'rowcount', cnt)

    def _load_token_blob(self, name):
        blob = self.r.get(b"tok:"+name)
        if blob is None:
            raise KeyError
        return blob

    def save_token(self, name, record_ids):
        self.r.set(b"tok:"+name, array("L", record_ids).tobytes())

    def save_tokens(self, names_ids, chunk_size=5000):
        chunks = partition_all(chunk_size, names_ids)
        for chunk in chunks:
            pipe = self.r.pipeline()
            for name, record_ids in chunk:
                pipe.set(b"tok:"+name, array("L", record_ids).tobytes())
            pipe.execute()

    def _load_record_blob(self, idx):
        blob = self.r.get(array("L", (idx,)).tobytes())
        if blob is None:
            raise KeyError
        return blob

    def get_records(self, idxs, chunk_size=5000):
        chunks = partition_all(chunk_size, idxs)
        for chunk in chunks:
            keys = [array("L", (idx,)).tobytes() for idx in chunk]
            blobs = self.r.mget(keys)
            if any(blob is None for blob in blobs):
                raise KeyError
            for blob in blobs:
                yield self._get_record(blob)

    def save_record(self, rec, idx=None, save_rowcount=True):
        if not idx or save_rowcount is True:
            idx = self.r.incr(b'rowcount')
        self.r.set(array("L", (idx,)).tobytes(), dumps(rec))
        return idx

    def save_records(self, idx_recs, chunk_size=5000):
        chunks = partition_all(chunk_size, idx_recs)
        tot = 0
        for chunk in chunks:
            tot += len(chunk)
            pipe = self.r.pipeline()
            for idx, rec in chunk:
                pipe.set(array("L", (idx,)).tobytes(), dumps(rec))
            pipe.execute()
        return tot

    def delete_record(self, idx):
        self.r.delete(array("L", (idx,)).tobytes())


polymr.storage.backends['redis'] = RedisBackend
