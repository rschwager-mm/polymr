import time
import operator
import concurrent.futures
from array import array
from itertools import repeat
from itertools import count as counter
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor

import polymr.storage
from polymr.storage import dumps
from toolz import partition_all
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.exception import JSONResponseError
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
    BLOCK_SIZE = 1024*399
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
            throughput=dict(read=25, write=2000)
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
        item = self.table.get_item(primary=b'Featurizer', secondary=0,
                                   consistent=self.consistent)
        return item['name'].decode()

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
        chunks = partition_all(25, freqs_dict.items())
        for chunk in chunks:
            with self.table.batch_write() as batch:
                for tok, freq in chunk:
                    batch.put_item(data={'primary': b'freq:'+tok,
                                         'freq': freq, 'secondary': 0}, 
                                   overwrite=True)

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
        items = self.table.query_2(primary__eq=name or b'null')
        blob = bytearray()
        for item in items:
            blob.extend(item['bytes'])
        return blob

    def save_token(self, name, record_ids, batch=None):
        saver = batch or self.table
        blob = array("L", record_ids).tobytes() 
        for i, chunk in enumerate(_partition_bytes(blob, self.BLOCK_SIZE)):
            saver.put_item(data={'primary': name or b'null', 'secondary': i, 
                                 'bytes': chunk}, overwrite=True)

    def _save_multithreaded(self, saver_func, chunks, threads):
        tot = 0
        tables = [Table(self.table_name, schema=self.SCHEMA) for _ in range(threads)]
        with ThreadPoolExecutor(max_workers=threads) as executor:
            not_done = set(executor.submit(saver_func, chunk, tables[i], i)
                           for i, chunk in zip(range(threads), chunks))
            while True:
                done, not_done = concurrent.futures.wait(
                    not_done, return_when=FIRST_COMPLETED)
                for future in done:
                    i, cnt = future.result()
                    tot += cnt
                    try:
                        chunk = next(chunks)
                    except StopIteration:
                        for future in concurrent.futures.as_completed(not_done):
                            _, cnt = future.result()
                            tot += cnt
                        return tot
                    not_done.add(executor.submit(saver_func, chunk, tables[i], i))

    def save_tokens(self, names_ids_compacteds, chunk_size=25, threads=1):
        def _save(chunk, table, i):
            with table.batch_write() as batch:
                for name, record_ids, compacted in chunk:
                    self.save_token(name, record_ids, compacted, batch=batch)
            return i, 0

        chunks = partition_all(chunk_size, names_ids_compacteds)
        if threads > 1:
            return self._save_multithreaded(_save, chunks, threads)
        else:
            return sum(map(_save, chunks, repeat(self.table), repeat(0)))

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
        saver.put_item(data={'primary': array("L", (idx,)).tobytes(), 
                             'secondary': 0, 'bytes': dumps(rec)}, 
                       overwrite=True)
        if save_rowcount is True:
            self.save_rowcount(idx)
        return idx

    def save_records(self, idx_recs, record_db=None, chunk_size=25, threads=1):
        def _save(chunk, table, i):
            cnt = counter()
            with table.batch_write() as batch:
                for idx, rec in chunk:
                    self.save_record(rec, idx=idx, save_rowcount=False, 
                                     batch=batch)
                    next(cnt)
            return i, next(cnt)

        chunks = partition_all(chunk_size, idx_recs)
        if threads > 1:
            return self._save_multithreaded(_save, chunks, threads)
        else:
            return sum(cnt for _, cnt 
                       in map(_save, chunks, repeat(self.table), repeat(0)))

    def delete_record(self, idx):
        self.table.delete_item(primary=str(idx).encode(), secondary=0)


polymr.storage.backends['dynamodb'] = DynamoDBBackend
