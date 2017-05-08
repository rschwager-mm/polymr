import json
import queue
import logging
import traceback
import multiprocessing
from heapq import nsmallest
from base64 import b64encode
from collections import Counter
from collections import OrderedDict
from collections import defaultdict
from itertools import chain
from itertools import cycle
from operator import itemgetter

from . import score
from . import storage
from . import featurizers


first = itemgetter(0)
logger = logging.getLogger(__name__)
cat = chain.from_iterable


class defaults:
    n = 600
    r = int(1e5)
    k = None
    limit = 5


class Index(object):
    def __init__(self, backend):
        self.backend = backend
        self.rowcount = self.backend.get_rowcount()
        self.featurizer = featurizers.all[self.backend.featurizer_name]

    def _search(self, query, r, n, k):
        toks = [b64encode(t) for t in self.featurizer(query)]
        toks = self.backend.find_least_frequent_tokens(toks, r)
        r_map = Counter()
        for i, tok in enumerate(toks, 1):
            rng = self.backend.get_token(tok)
            r_map.update(rng)
            if k and i >= k: #  try to get k token mappings
                break
        top_ids = map(first, r_map.most_common(n))
        return list(top_ids)

    def _scored_records(self, record_ids, orig_query):
        orig_features = score.features(orig_query)
        for rownum, r in zip(record_ids, self.backend.get_records(record_ids)):
            s = score.hit(orig_features, score.features(r.fields))
            yield s, rownum, r

    def search(self, query, limit=defaults.limit, r=defaults.r, n=defaults.n,
               k=None):
        record_ids = self._search(query, r, n, k)
        scores_records = self._scored_records(record_ids, query)
        return [
            {"fields": rec.fields, "pk": rec.pk, "score": s,
             "data": rec.data, "rownum": rownum}
            for s, rownum, rec in nsmallest(limit, scores_records, key=first)
        ]

    def _save_records(self, records):
        completed = []
        for rec in records:
            try:
                idx = self.backend.save_record(rec)
            except:
                for idx in completed:
                    self.backend.delete_record(idx)
                raise
            completed.append(idx)
        self.backend.increment_rowcount(len(completed))
        return completed

    def _update_tokens(self, tokmap, freq_update):
        for tok in tokmap.keys():
            idxs = tokmap[tok]
            self.backend.update_token(tok, idxs)
            freq_update[tok] = len(idxs)

    def _update_tokens_and_freqs(self, tokmap):
        freq_update = {}
        try:
            self._update_tokens(tokmap, freq_update)
            self.backend.update_freqs(freq_update.items())
        except:
            for tok in freq_update:
                self.backend.drop_records_from_token(tok, tokmap[tok])
            raise

    def add(self, records):
        idxs = list(self._save_records(records))
        tokmap = defaultdict(list)
        for idx, rec in zip(idxs, records):
            for tok in map(b64encode, self.featurizer(rec.fields)):
                tokmap[tok].append(idx)
        self._update_tokens_and_freqs(tokmap)
        return idxs

    def close(self):
        return self.backend.close()


class ParallelIndexWorker(multiprocessing.Process):
    def __init__(self, backend_name, work_q, result_q):
        super().__init__()
        self.work_q = work_q
        self.result_q = result_q
        self.counters = dict()
        self.be_cls = storage.backends[backend_name]
        self.methods = dict(count_tokens=self._count_tokens,
                            score_records=self._score_records)

    def _count_tokens(self, query_key, n_total_toks, blob, n):
        r_map = self.counters.get(query_key, None)
        if not r_map:
            r_map = self.counters[query_key] = {'cntr': Counter(),
                                                'n_toks': 0}
        r_map['cntr'].update(self.be_cls._get_token(blob))
        r_map['n_toks'] += 1
        if r_map['n_toks'] == n_total_toks:
            cnt = self.counters.pop(query_key)['cntr']
            return list(map(first, cnt.most_common(n)))
        else:
            return None

    def _scores(self, blobs, orig_features):
        for rownum, blob in blobs:
            r = self.be_cls._get_record(blob)
            s = score.hit(orig_features, score.features(r.fields))
            yield s, rownum, r

    def _score_records(self, orig_features, limit, blobs):
        scores_records = self._scores(blobs, orig_features)
        return nsmallest(limit, scores_records, key=first)

    def run(self):
        while True:
            try:
                logger.debug("Getting work")
                query_id, meth, args = self.work_q.get()
            except IOError as e:
                logger.debug("Received IOError (%s) errno %s from work_q",
                             e.message, e.errno)
                break
            except EOFError:
                logger.debug("Received EOFError from work_q")
                break
            if meth == 'stop':
                logger.debug("Received sentinel, stopping")
                break
            try:
                logger.debug("Running method %s", meth)
                ret = self.methods[meth](*args)
            except Exception as e:
                ret = e
                traceback.print_exc()
                pass
            if ret:
                logger.debug("Finished, putting result on q")
                self.result_q.put_nowait((query_id, meth, ret))
                logger.debug("Result put on result_q. Back to get more work.")
            else:
                logger.debug("Method returned None. Back to get more work.")


class ParallelIndex(Index):
    def __init__(self, backend_url, n_workers):
        parsed = storage.urlparse(backend_url)
        self.backend_name = parsed.scheme
        self.n_workers = n_workers
        self.backend = storage.backends[parsed.scheme].from_urlparsed(parsed)
        self.worker_rot8 = cycle(range(n_workers))
        self.featurizer = featurizers.all[self.backend.featurizer_name]
        self.started = False

    def _startup_workers(self):
        logger.debug("Starting %s workers", self.n_workers)
        self.work_qs = [multiprocessing.Queue() for _ in range(self.n_workers)]
        self.result_q = multiprocessing.Queue()
        self.workers = [
            ParallelIndexWorker(self.backend_name, work_q, self.result_q)
            for work_q in self.work_qs
        ]
        for worker in self.workers:
            worker.start()
        return True

    def _search(self, query_id, query, r, n, k):
        which_worker = next(self.worker_rot8)
        toks = [b64encode(t) for t in self.featurizer(query)]
        toks = self.backend.find_least_frequent_tokens(toks, r)
        for i, tok in enumerate(toks, 1):
            blob = self.backend._load_token_blob(tok)
            self.work_qs[which_worker].put(
                (query_id, 'count_tokens', [query_id, len(toks), blob, n])
            )
            if k and i >= k:
                break
        return which_worker

    def _scored_records(self, query_id, record_ids, query, limit):
        which_worker = next(self.worker_rot8)
        orig_features = score.features(query)
        blobs = [(i, self.backend._load_record_blob(i)) for i in record_ids]
        self.work_qs[which_worker].put(
            (query_id, 'score_records', [orig_features, limit, blobs])
        )
        return which_worker

    @staticmethod
    def _format_resultset(scores_recs):
        return [{"fields": rec.fields, "pk": rec.pk, "score": s,
                 "data": rec.data, "rownum": rownum}
                for s, rownum, rec in scores_recs]

    def search(self, query, limit=defaults.limit, r=defaults.r,
               n=defaults.n, k=defaults.k):
        self._search(0, query, r, n)
        _, _, record_ids = self.result_q.get()
        self._scored_records(0, record_ids, query, limit)
        _, _, scores_recs = self.result_q.get()
        return self._format_resultset(scores_recs)

    def _fill_work_queues(self, r, n, k):
        n_filled = 0
        while len(self.in_progress) < len(self.workers)*3 and self.to_do:
            query_id, query = self.to_do.popitem(last=False)
            self._search(query_id, query, r, n, k)
            self.in_progress[query_id] = query
            n_filled += 1
        logger.debug("Added %i tasks to work queues", n_filled)

    def _searchmany(self, queries, limit, r, n, k):
        self.to_do = OrderedDict(enumerate(queries))
        self.in_progress = {}
        send_later = {}  # query_id : search results
        n_sent = 0
        while any((self.in_progress, self.to_do, send_later)):
            self._fill_work_queues(r, n, k)
            try:
                query_id, meth, ret = self.result_q.get()
            except queue.Empty:
                logger.debug("Result q empty.")
                continue
            if isinstance(ret, Exception):
                logger.warning("Hit exception while processing query %i: %s",
                               query_id, ret)
                send_later[query_id] = ret
                del self.in_progress[query_id]
                continue
            if meth == 'count_tokens':
                logger.debug('count_tokens completed for query %s', query_id)
                query = queries[query_id]
                self._scored_records(query_id, ret, query, limit)
                self.in_progress[query_id] = query
            elif meth == 'score_records':
                logger.debug('score_records completed for query %s', query_id)
                send_later[query_id] = ret
                del self.in_progress[query_id]
                while n_sent in send_later:
                    logger.debug('Sending resultset %s', n_sent)
                    yield self._format_resultset(send_later.pop(n_sent))
                    logger.info("Completed query %i", n_sent)
                    logger.debug((self.in_progress, self.to_do, send_later))
                    logger.debug(
                        "Any left to do? %s",
                        any((self.in_progress, self.to_do, send_later)))
                    n_sent += 1

    def searchmany(self, queries, limit=defaults.limit, r=defaults.r,
                   n=defaults.n, k=defaults.k):
        self.started = self._startup_workers()
        try:
            for result in self._searchmany(queries, limit, r, n, k):
                yield result
        finally:
            self.close(close_backend=False)

    def close(self, timeout=None, close_backend=True):
        if close_backend is True:
            self.backend.close()
        logger.debug("Shutting down workers")
        for i, work_q in enumerate(self.work_qs):
            work_q.put((0, 'stop', []), timeout or 0)
        for i, worker in enumerate(self.workers):
            logger.debug("Joining worker %i", i)
            worker.join()
        logger.debug("Shutdown complete")


class CLI:

    name = "query"

    arguments = [
        (["term"], {
            "type": str,
            "nargs": "+"
        }),
        storage.backend_arg,
        (["-r", "--seeds"], {
            "type": int,
            "help": "The number of record votes to tally.",
            "default": defaults.r}),
        (["-n", "--search-space"], {
            "type": int,
            "help": ("The number of seed records to search through "
                     "for best matches"),
            "default": defaults.n}),
        (["-l", "--limit"], {
            "type": int,
            "help": "The number of search results to return",
            "default": defaults.limit
        })
    ]

    @staticmethod
    def hook(parser, args):
        backend = storage.parse_url(args.backend)
        index = Index(backend)
        results = index.search(
            args.term,
            limit=args.limit, k=args.seeds, n=args.search_space
        )
        print(json.dumps(results, indent=2))
