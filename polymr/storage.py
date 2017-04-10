import os
import logging
from collections import defaultdict
from abc import ABCMeta
from abc import abstractmethod
from urllib.parse import urlparse

import leveldb
import msgpack

from .record import Record


logger = logging.getLogger(__name__)


def loads(bs):
    return msgpack.unpackb(bs)


def dumps(obj):
    return msgpack.packb(obj)


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

        :rtype: dict {str: int}
        """
        ...

    @abstractmethod
    def save_freqs(self, d):
        """Save the frequency dict.

        :param d: The dict consisting of tokens and the number of
          records containing that token
        :type d: dict {str: int}
        """
        ...

    @abstractmethod
    def get_rowcount(self):
        """Get the number of records indexed

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
        :type name: str

        :returns: The list of records containing that token
        :rtype: list

        """
        ...

    @abstractmethod
    def save_token(self, name, record_ids, compacted):
        """Save the list of records containing a named token

        :param name: The token
        :type name: str

        :param record_ids: The list of record ids containing the token
        :type record_ids: list of int (or list-of-list-of-int if
          compacted is True)

        :param compacted: Whether the records list is compacted into
          ranges. If True, ``records`` is expected to be a mixed list
          of ints and list-of-int ranges. E.g. ``records = [1, 3
          [5,10], 12]``
        :type compacted: bool

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
    def save_records(self, idx_recs):
        """Save records.

        :param idx_recs: The record id, record pairs to save
        :type idx_recs: iterable of (int, record) pairs.

        :returns: The number of rows saved
        :rtype: int
        """
        ...


class LevelDBBackend(AbstractBackend):
    def __init__(self, path, create_if_missing=True,
                 featurizer_name='default'):
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
                name = self.get_featurizer_name(self.path)
            except:
                name = 'default'
            self.featurizer_name = name
        self._check_dbstats()

    @staticmethod
    def get_featurizer_name(path):
        with open(os.path.join(path, "featurizer")) as f:
            name = f.read()
        return name

    def _check_dbstats(self):
        try:
            self.get_freqs()
        except KeyError:
            self.save_freqs({})
        try:
            self.get_rowcount()
        except KeyError:
            self.save_rowcount(0)
        with open(os.path.join(self.path, "featurizer"), 'w') as f:
            f.write(self.featurizer_name)

    @classmethod
    def from_urlparsed(cls, parsed):
        return cls(parsed.path)

    def close(self):
        del self.feature_db
        self.feature_db = None
        del self.record_db
        self.record_db = None

    def get_freqs(self):
        s = self.feature_db.Get("Freqs".encode())
        return defaultdict(int, loads(s))

    def save_freqs(self, freqs_dict):
        self.feature_db.Put("Freqs".encode(), dumps(freqs_dict))

    def get_rowcount(self):
        return loads(self.record_db.Get("Rowcount".encode()))

    def save_rowcount(self, cnt):
        self.record_db.Put("Rowcount".encode(), dumps(cnt))

    @staticmethod
    def _get_token(blob):
        ret = loads(blob)
        if ret[b'compacted'] is False:
            return ret[b'idxs']
        idxs = []
        for idx in ret[b'idxs']:
            if type(idx) is list:
                idxs.extend(list(range(idx[0], idx[1]+1)))
            else:
                idxs.append(idx)
        return idxs

    def _load_token_blob(self, name):
        return self.feature_db.Get(name)

    def get_token(self, name):
        blob = self._load_token_blob(name)
        return self._get_token(blob)

    def save_token(self, name, record_ids, compacted):
        self.feature_db.Put(
            name,
            dumps({b"idxs": record_ids, b"compacted": compacted})
        )

    @staticmethod
    def _get_record(blob):
        rec = loads(blob)
        rec[0] = list(map(bytes.decode, rec[0]))
        return Record._make(rec)

    def _load_record_blob(self, idx):
        return self.record_db.Get(str(idx).encode())

    def get_records(self, idxs):
        for idx in idxs:
            blob = self._load_record_blob(idx)
            yield self._get_record(blob)

    def save_records(self, idx_recs, record_db=None):
        for cnt, (idx, rec) in enumerate(idx_recs):
            self.record_db.Put(
                str(idx).encode(),
                dumps(rec)
            )
        return cnt+1

    def delete_record(self, idx):
        self.record_db.Delete(str(idx).encode())


backends = {"leveldb": LevelDBBackend}


def parse_url(u):
    parsed = urlparse(u)
    if parsed.scheme not in backends:
        raise ValueError("Unrecognized scheme: "+parsed.scheme)
    return backends[parsed.scheme].from_urlparsed(parsed)


backend_arg = (["-b", "--backend"], {
    "type": str,
    "help": ("URL for storage backend. Currently only supports "
             "`leveldb://localhost/path/to/db'"),
    "required": True
})
