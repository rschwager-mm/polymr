import os
import logging
import operator
from array import array
from abc import ABCMeta
from abc import abstractmethod
from functools import partial
from collections import defaultdict
from itertools import count as counter
from urllib.parse import urlparse

import leveldb
import msgpack
from toolz import partition_all

from .record import Record

logger = logging.getLogger(__name__)
_isinfo = logger.isEnabledFor(logging.INFO)
snd = operator.itemgetter(1)


def loads(bs):
    return msgpack.unpackb(bs)


def dumps(obj):
    return msgpack.packb(obj)


def copy(backend_from, backend_to, droptop=None,
         skip_copy_records=False, skip_copy_featurizer=False,
         skip_copy_freqs=False, skip_copy_tokens=False, threads=None):
    logger.debug("Copying from %s to %s", backend_from, backend_to)
    if threads is not None:
        save_records = partial(backend_to.save_records, threads=threads)
        save_tokens = partial(backend_to.save_tokens, threads=threads)
    else:
        save_records = backend_to.save_records
        save_tokens = backend_to.save_tokens
    if skip_copy_records is False:
        cnt = backend_from.get_rowcount()
        recs = backend_from.get_records(range(0, cnt))
        logger.info("Copying %i records", cnt)
        save_records(enumerate(recs))
    if skip_copy_freqs is False:
        logger.info("Copying frequencies")
    if any((skip_copy_freqs is False,
            droptop is not None,
            skip_copy_tokens is False)):
        freqs = backend_from.get_freqs()
    if droptop is not None:
        thresh = int(len(freqs) * float(droptop))
        freqs = sorted(freqs.items(), key=operator.itemgetter(1),
                       reverse=True)[thresh:]
        freqs = dict(freqs)
    if skip_copy_freqs is False:
        backend_to.save_freqs(freqs)
    if skip_copy_featurizer is False:
        logger.info("Copying featurizer name")
        backend_to.save_featurizer_name(backend_from.get_featurizer_name())

    def _rows():
        for i, tok in enumerate(freqs):
            idxs = backend_from.get_token(tok)
            yield (tok, idxs)
            if _isinfo:
                n_bins = len(freqs) // 100
                if (i % n_bins) == 0:
                    logger.info("Feature copy %.2f%% complete",
                                i / len(freqs) * 100)

    if skip_copy_tokens is False:
        logger.info("Copying features")
        save_tokens(_rows())
    logger.info("Copy complete")


class AbstractBackend(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def from_urlparsed(cls, parsed):
        ...

    @abstractmethod
    def close(self):
        ...

    @abstractmethod
    def get_freqs(self):
        """Get a the freqeuency dict

        :returns: dict consisting of tokens and the number of records
          containing that token

        :rtype: dict {bytes: int}
        """
        ...

    @abstractmethod
    def save_freqs(self, d):
        """Save the frequency dict.

        :param d: The dict consisting of tokens and the number of
          records containing that token
        :type d: dict {bytes: int}
        """
        ...

    @abstractmethod
    def get_rowcount(self):
        """Get the number of records indexed

        :rtype: int
        """
        ...

    @abstractmethod
    def increment_rowcount(self, n):
        """Increase the rowcount

        :param n: The number by which to increase the rowcount
        :type n: int

        :rtype: int
        """
        ...

    @abstractmethod
    def save_rowcount(self, cnt):
        """Save the number of records indexed

        :param cnt: The row count to save
        :type cnt: int
        """
        ...

    @abstractmethod
    def get_token(self, name):
        """Get the list of records containing the named token

        :param name: The token to get
        :type name: bytes

        :returns: The list of records containing that token
        :rtype: list

        """
        ...

    @abstractmethod
    def update_token(self, name, record_ids):
        """Update the list of record ids corresponding to a token.

        :param name: The token
        :type name: bytes

        :param record_ids: The list of record ids containing the token
        :type record_ids: list of int

        """
        ...

    @abstractmethod
    def drop_records_from_token(self, name, bad_record_ids):
        """Remove bad record IDs from the list of IDs associated with a token

        :param name: The token
        :type name: bytes

        :param bad_record_ids: The record ids to remove
        :type bad_record_ids: list of int

        """
        ...

    @abstractmethod
    def save_token(self, name, record_ids):
        """Save the list of records containing a named token

        :param name: The token
        :type name: bytes

        :param record_ids: The list of record ids containing the token
        :type record_ids: iterable of int

        """
        ...

    @abstractmethod
    def save_tokens(self, names_ids):
        """Save many tokens in bulk. See ``save_token``.

        :param names_ids: A iterable of two-part tuples: the token,
          and the ids corresponding to the token
        :type names_ids: iterable of tuple

        """
        ...

    @abstractmethod
    def get_record(self, idx):
        """Gets a record with a record id

        :param idx: The id of the record to retreive
        :type idx: int

        """
        ...

    @abstractmethod
    def get_records(self, idxs):
        """Get records by record id

        :param idxs: The ids of the records to retreive
        :type idxs: list of int

        """
        ...

    @abstractmethod
    def save_record(self, rec):
        """Save records.

        :param rec: The record to save
        :type rec: polymr.record.Record

        :returns: The ID of the newly created record
        :rtype: int
        """
        ...

    @abstractmethod
    def save_records(self, idx_recs):
        """Save records.

        :param idx_recs: The record id, record pairs to save
        :type idx_recs: iterable of (int, record) pairs.

        :returns: The number of rows saved
        :rtype: int
        """
        ...


class LevelDBBackend(AbstractBackend):
    def __init__(self, path=None,
                 create_if_missing=True,
                 featurizer_name=None,
                 feature_db=None,
                 record_db=None):
        self._freqs = None
        if feature_db is not None or record_db is not None:
            self.feature_db = feature_db
            self.record_db = record_db
            self.path = None
            return

        self.path = path
        if create_if_missing and not os.path.exists(path):
            os.mkdir(path)

        self.feature_db = leveldb.LevelDB(os.path.join(path, "features"),
                                          create_if_missing=create_if_missing)
        self.record_db = leveldb.LevelDB(os.path.join(path, "records"),
                                         create_if_missing=create_if_missing)
        self.featurizer_name = featurizer_name
        if not self.featurizer_name:
            try:
                name = self.get_featurizer_name()
            except:
                name = 'default'
            self.featurizer_name = name
        self._check_dbstats()

    def get_featurizer_name(self):
        with open(os.path.join(self.path, "featurizer")) as f:
            name = f.read()
        return name

    def save_featurizer_name(self, name):
        with open(os.path.join(self.path, "featurizer"), 'w') as f:
            f.write(name)

    def _check_dbstats(self):
        try:
            self.get_freqs()
        except KeyError:
            self.save_freqs({})
        try:
            self.get_rowcount()
        except KeyError:
            self.save_rowcount(0)
        try:
            self.get_featurizer_name()
        except OSError:
            self.save_featurizer_name('default')

    @classmethod
    def from_urlparsed(cls, parsed, featurizer_name=None):
        return cls(parsed.path, featurizer_name=featurizer_name)

    def close(self):
        del self.feature_db
        self.feature_db = None
        del self.record_db
        self.record_db = None

    def find_least_frequent_tokens(self, toks, r, k=None):
        if not self._freqs:
            self._freqs = self.get_freqs()
        toks_freqs = [(tok, self._freqs[tok]) for tok in toks
                      if tok in self._freqs]
        total = 0
        ret = []
        for i, (tok, freq) in enumerate(sorted(toks_freqs, key=snd)):
            if total + freq > r:
                break
            total += freq
            ret.append(tok)
            if k and i >= k:  # try to get k token mappings
                break
        return ret

    def get_freqs(self):
        s = self.feature_db.Get("Freqs".encode())
        return defaultdict(int, loads(s))

    def update_freqs(self, toks_cnts):
        if not self._freqs:
            self._freqs = self.get_freqs()
        self._freqs.update(toks_cnts)
        self.save_freqs(self._freqs)

    def save_freqs(self, freqs_dict):
        self.feature_db.Put("Freqs".encode(), dumps(freqs_dict))

    def get_rowcount(self):
        return loads(self.record_db.Get("Rowcount".encode()))

    def increment_rowcount(self, n):
        current_rowcount = self.get_rowcount()
        self.save_rowcount(current_rowcount + n)

    def save_rowcount(self, cnt):
        self.record_db.Put("Rowcount".encode(), dumps(cnt))

    @staticmethod
    def _get_token(blob):
        return array("L", blob)

    def _load_token_blob(self, name):
        return self.feature_db.Get(name)

    def get_token(self, name):
        blob = self._load_token_blob(name)
        return self._get_token(blob)

    def update_token(self, name, record_ids):
        try:
            curidxs = self.get_token(name)
        except KeyError:
            # possible the token is new
            curidxs = []
        self.save_token(name, curidxs+record_ids)

    def drop_records_from_token(self, name, bad_record_ids):
        curidxs = self.get_token(name)
        to_keep = list(set(curidxs)-set(bad_record_ids))
        self.save_token(name, to_keep)

    def save_token(self, name, record_ids):
        self.feature_db.Put(name, array("L", record_ids).tobytes())

    def save_tokens(self, names_ids, chunk_size=5000):
        chunks = partition_all(chunk_size, names_ids)
        for chunk in chunks:
            batch = leveldb.WriteBatch()
            for name, record_ids in chunk:
                batch.Put(name, array("L", record_ids).tobytes())
            self.feature_db.Write(batch)

    @staticmethod
    def _get_record(blob):
        rec = loads(blob)
        rec[0] = list(map(bytes.decode, rec[0]))
        rec[1] = rec[1].decode()
        return Record._make(rec)

    def _load_record_blob(self, idx):
        return self.record_db.Get(array("L", (idx,)).tobytes())

    def get_record(self, idx):
        blob = self._load_record_blob(idx)
        return self._get_record(blob)

    def get_records(self, idxs):
        for idx in idxs:
            blob = self._load_record_blob(idx)
            yield self._get_record(blob)

    def save_record(self, rec, idx=None, save_rowcount=True):
        idx = self.get_rowcount() + 1 if idx is None else idx
        self.record_db.Put(array("L", (idx,)).tobytes(), dumps(rec))
        if save_rowcount is True:
            self.save_rowcount(idx)
        return idx

    update_record = save_record

    def save_records(self, idx_recs, record_db=None, chunk_size=5000):
        chunks = partition_all(chunk_size, idx_recs)
        cnt = counter()
        for chunk in chunks:
            batch = leveldb.WriteBatch()
            for idx, rec in chunk:
                batch.Put(array("L", (idx,)).tobytes(), dumps(rec))
                next(cnt)
            self.record_db.Write(batch)
        return next(cnt)

    def delete_record(self, idx):
        self.record_db.Delete(array("L", (idx,)).tobytes())


backends = {"leveldb": LevelDBBackend}


def parse_url(u, **kwargs):
    parsed = urlparse(u)
    if parsed.scheme not in backends:
        raise ValueError("Unrecognized scheme: "+parsed.scheme)
    return backends[parsed.scheme].from_urlparsed(parsed, **kwargs)


backend_arg = (["-b", "--backend"], {
    "type": str,
    "help": ("URL for storage backend. Currently only supports "
             "`leveldb://localhost/path/to/db'"),
    "required": True
})
