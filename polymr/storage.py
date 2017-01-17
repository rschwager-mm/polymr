import os
import json
from abc import ABCMeta
from abc import abstractmethod
from urllib.parse import urlparse

import leveldb

from .record import Record


def loads(bs):
    return json.loads(bs.decode())

def dumps(obj):
    return json.dumps(obj).encode()


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
    def get_tokprefix(self, name):
        """Get all tokens and record lists saved with the prefix ``name +"_"``
        This is used when merging record lists across multiple feature
        tables into a single record list.

        :param name: The token to get
        :type name: str

        :returns: The list of suffixed tokens and the list of record lists
        :rtype: (list of str, list of ints)

        """

        ...


    @abstractmethod
    def save_tokprefix(self, toks_idxs_rngs):
        """Save token and record ranges for merging feature tables.

        :param toks_idxs_rngs: The token, table number, and record
          lists from the feature table to save
        :type toks_idxs_rngs: Iterable of (str, int, list-of-int)
          tuples.

        """
        ...


    @abstractmethod
    def delete_tokprefix(self, name):
        """Delete all tokens and record lists saved with the prefix ``name
        +"_"``. This is used when merging record lists across multiple
        feature tables into a single record list.

        :param name: The token to delete
        :type name: str

        :returns: The number of token chunks deleted
        :rtype: int

        """
        ...


    @abstractmethod
    def get_toklist(self):
        """Get the list of all tokens saved in the backend

        :rtype: list of str

        """
        ...


    @abstractmethod
    def save_toklist(self, toks):
        """Save the list of all tokens saved in the backend
        
        :param toks: The list of all tokens
        :type toks: list of str

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
    def __init__(self, path, create_if_missing=True):
        c = create_if_missing
        self.feature_db = leveldb.LevelDB(os.path.join(path, "features"),
                                          create_if_missing=c)
        self.record_db = leveldb.LevelDB(os.path.join(path, "records"),
                                         create_if_missing=c)


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
        return loads(s)


    def save_freqs(self, freqs_dict):
        self.feature_db.Put("Freqs".encode(), dumps(freqs_dict))


    def get_rowcount(self):
        return loads( self.record_db.Get("Rowcount".encode()) )


    def save_rowcount(self, cnt):
        self.record_db.Put("Rowcount".encode(), dumps(cnt))


    def get_token(self, name):
        ret = loads(self.feature_db.Get(name.encode()))
        if ret['compacted'] is False:
            return ret['idxs']
        idxs = []
        for idx in ret['idxs']:
            if type(idx) is list:
                idxs.extend(list(range(idx[0], idx[1]+1)))
            else:
                idxs.append(idx)
        return idxs


    def save_token(self, name, record_ids, compacted):
        self.feature_db.Put(
            name.encode(),
            dumps({"idxs": record_ids, "compacted": compacted})
        )


    def get_tokprefix(self, name):
        keys, rngs = [], []
        for k, v in self.feature_db.RangeIter((name+"_").encode(), None):
            key = k.decode()
            if not key.startswith(name+"_"):
                break
            keys.append(key)
            rngs.append(loads(v))
        return keys, rngs


    def save_tokprefix(self, toks_idxs_rngs):
        for tok, idx, rng in toks_idxs_rngs:
            self.feature_db.Put(
                "{}_{}".format(tok, idx).encode(),
                dumps(rng)
            )


    def delete_tokprefix(self, name):
        cnt = 0
        for k, v in self.feature_db.RangeIter((name+"_").encode(), None):
            key = k.decode()
            if not key.startswith(name+"_"):
                break
            self.feature_db.Delete(k)
            cnt += 1
        return cnt


    def get_toklist(self):
        return loads(self.feature_db.Get("Tokens".encode()))


    def save_toklist(self, toks):
        self.feature_db.Put("Tokens".encode(), dumps(toks))

        
    def get_records(self, idxs):
        for idx in idxs:
            d = loads(self.record_db.Get(str(idx).encode()))
            yield Record(**d)


    def save_records(self, idx_recs):
        for cnt, (idx, rec) in enumerate(idx_recs):
            self.record_db.Put(
                str(idx).encode(),
                dumps(rec._asdict())
            )
        return cnt+1


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
