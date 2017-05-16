import os
import time
import operator
from itertools import count as counter
from collections import namedtuple
from collections import defaultdict

import polymr.storage
from polymr.storage import loads
from polymr.storage import dumps
from toolz import partition_all
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.exception import JSONResponseError
from boto.dynamodb2.exceptions import ValidationException
from boto.dynamodb2.exceptions import ItemNotFound
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.types import BINARY


snd = operator.itemgetter(1)


def _partition_bytes(bs, size):
    while bs:
        chunk, bs = bs[:size], bs[size:]
        yield chunk


class BackendError(EnvironmentError):
    pass


class DynamoDBBackend(polymr.storage.LevelDBBackend):
    BLOCK_SIZE = 1024*400
    SCHEMA = [HashKey('primary', data_type=BINARY),
              RangeKey('secondary', data_type=NUMBER)]

    def __init__(self, table_name=None,
                 create_if_missing=True,
                 featurizer_name=None,
                 consistent=False):
        self.consistent = consistent
        self.table_name = table_name
        try:
            self.table = Table(self.table_name, schema=self.SCHEMA)
            self.table.describe()
        except JSONResponseError as e:
            if 'not found' in e.message and create_if_missing:
                self.create()
            else:
                raise

        self.featurizer_name = featurizer_name
        if not self.featurizer_name:
            try:
                name = self.get_featurizer_name()
            except:
                name = 'default'
            self.featurizer_name = name
        self._check_dbstats()

    def create(self):
        self.table = Table.create(
            self.table_name, 
            schema=self.SCHEMA,
            throughput=dict(read=25, write=25)
        )
        for _ in range(100):
            try:
                self.get_rowcount()
            except ItemNotFound:
                return
            except JSONResponseError as e:
                if 'not found' in e.message:
                    time.sleep(0.3)
        raise BackendError
        
    def destroy(self):
        self.table.delete()
    
    @classmethod
    def from_urlparsed(cls, parsed, featurizer_name=None):
        return cls(parsed.path.split('/')[-1], featurizer_name=featurizer_name)

    def get_featurizer_name(self):
        return self.table.get_item(primary=b'Featurizer', secondary=0,
                                   consistent=self.consistent)['name']

    def save_featurizer_name(self, name):
        self.table.put_item(data={'primary': b'Featurizer', 'secondary': 0,
                                  'name': name.encode()}, overwrite=True)

    def _check_dbstats(self):
        try:
            self.get_rowcount()
        except ItemNotFound:
            self.save_rowcount(0)
        try:
            self.get_featurizer_name()
        except ItemNotFound:
            self.save_featurizer_name('default')

    def get_freqs(self):
        # TODO. Should only be used for copying out of dynamodb.
        pass

    def update_freqs(self, toks_cnts):
        self.save_freqs(dict(toks_cnts))

    def save_freqs(self, freqs_dict):
        with self.table.batch_write() as batch:
            for tok, freq in freqs_dict.items():
                batch.put_item(data={'primary': b'freq:'+tok, 'freq': freq,
                                     'secondary': 0}, overwrite=True)

    def find_least_frequent_tokens(self, toks, r, k=None):
        keys=[{'primary': b'freq:'+tok, 'secondary': 0} for tok in toks]
        toks_freqs = [(item['primary'][len('freq:'):], int(item['freq']))
                      for item in self.table.batch_get(keys=keys)]
        total = 0
        ret = []
        for i, (tok, freq) in enumerate(sorted(toks_freqs, key=snd)):
            if total + freq > r:
                break
            total += freq
            ret.append(tok)
            if k and i >= k: #  try to get k token mappings
                break
        return ret

    def get_rowcount(self):
        item = self.table.get_item(primary=b"Rowcount", secondary=0, 
                                   consistent=self.consistent)
        if item is None or 'cnt' not in item:
            raise KeyError
        return int(item['cnt'])

    def save_rowcount(self, cnt):
        self.table.put_item(data={'primary': b'Rowcount', 'cnt': cnt,
                                  'secondary': 0}, overwrite=True)

    def _load_token_blob(self, name):
        items = self.table.query_2(primary__eq=name)
        blob = bytearray()
        for item in items:
            blob.extend(item['bytes'])
        return blob

    def save_token(self, name, record_ids, compacted, batch=None):
        saver = batch or self.table
        blob = dumps({b"idxs": record_ids, b"compacted": compacted})
        for i, chunk in enumerate(_partition_bytes(blob, self.BLOCK_SIZE)):
            saver.put_item(data={'primary': name, 'secondary': i, 'bytes': chunk},
                           overwrite=True)

    def save_tokens(self, names_ids_compacteds, chunk_size=None):
        with self.table.batch_write() as batch:
            for name, record_ids, compacted in names_ids_compacteds:
                self.save_token(name, record_ids, compacted, batch=batch)

    def _load_record_blob(self, idx):
        item = self.table.get_item(primary=str(idx).encode(), secondary=0,
                                   consistent=self.consistent)
        if item or 'bytes' not in item:
            raise KeyError
        return item['bytes']

    def get_records(self, idxs):
        items = self.table.batch_get(
            keys=[{'primary': str(idx).encode(), 'secondary': 0}
                  for idx in idxs]
        )
        for item in items:
            blob = item['bytes']
            if blob is None:
                raise KeyError
            yield self._get_record(blob)

    def save_record(self, rec, idx=None, save_rowcount=True, batch=None):
        idx = self.get_rowcount() + 1 if idx is None else idx
        saver = batch or self.table
        saver.put_item(data={'primary': str(idx).encode(), 'secondary': 0,
                             'bytes': dumps(rec)}, overwrite=True)
        if save_rowcount is True:
            self.save_rowcount(idx)
        return idx

    def save_records(self, idx_recs, record_db=None, chunk_size=None):
        with self.table.batch_write() as batch:
            for i, (idx, rec) in enumerate(idx_recs, 1):
                self.save_record(rec, idx=idx, save_rowcount=False, batch=batch)
        return i

    def delete_record(self, idx):
        self.table.delete_item(primary=str(idx).encode(), secondary=0)


polymr.storage.backends['dynamodb'] = DynamoDBBackend
