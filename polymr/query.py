import json
import queue
import logging
import traceback
import multiprocessing
from heapq import nsmallest
from base64 import b64encode
from collections import Counter
from collections import OrderedDict
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
    k = 12
    limit = 5


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
        for blob in blobs:
            r = self.be_cls._get_record(blob)
            s = score.hit(orig_features, score.features(r.fields))
            yield s, r

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


class ParallelIndex(object):
    def __init__(self, backend_url, n_workers):
        parsed = storage.urlparse(backend_url)
        self.backend = storage.backends[parsed.scheme].from_urlparsed(parsed)
        self.worker_rot8 = cycle(range(n_workers))
        self.started = self._startup_workers(n_workers, parsed.scheme)
        self.freqs = self.backend.get_freqs()
        logger.debug("got %s freqs", len(self.freqs))
        self.featurizer = featurizers.all[self.backend.featurizer_name]

    def _startup_workers(self, n_workers, backend_name):
        logger.debug("Starting %s workers", n_workers)
        self.work_qs = [multiprocessing.Queue() for _ in range(n_workers)]
        self.result_q = multiprocessing.Queue()
        self.workers = [
            ParallelIndexWorker(backend_name, work_q, self.result_q)
            for work_q in self.work_qs
        ]
        for worker in self.workers:
            worker.start()
        return True

    def _search(self, query_id, query, k, n):
        which_worker = next(self.worker_rot8)
        toks = [b64encode(t) for t in self.featurizer(query)]
        toks = sorted(filter(self.freqs.__contains__, toks),
                      key=self.freqs.__getitem__)[:k]
        for tok in toks:
            blob = self.backend._load_token_blob(tok)
            self.work_qs[which_worker].put(
                (query_id, 'count_tokens', [query_id, len(toks), blob, n])
            )
        return which_worker

    def _scored_records(self, query_id, record_ids, query, limit):
        which_worker = next(self.worker_rot8)
        orig_features = score.features(query)
        blobs = [self.backend._load_record_blob(i) for i in record_ids]
        self.work_qs[which_worker].put(
            (query_id, 'score_records', [orig_features, limit, blobs])
        )
        return which_worker

    @staticmethod
    def _format_resultset(scores_recs):
        return [{"fields": rec.fields, "pk": rec.pk.decode(), "score": s,
                 "data": rec.data} for s, rec in scores_recs]

    def search(self, query, limit=defaults.limit, k=defaults.k, n=defaults.n):
        self._search(query, k, n)
        _, _, record_ids = self.result_q.get()
        self._scored_records(record_ids, query, limit)
        _, _, scores_recs = self.result_q.get()
        return self._format_resultset(scores_recs)

    def _fill_work_queues(self, k, n):
        while len(self.in_progress) < len(self.workers)*3 and self.to_do:
            query_id, query = self.to_do.popitem(last=False)
            self._search(query_id, query, k, n)
            self.in_progress[query_id] = query

    def searchmany(self, queries, limit=defaults.limit, k=defaults.k,
                   n=defaults.n):
        self.to_do = OrderedDict(enumerate(queries))
        self.in_progress = {}
        send_later = {}  # query_id : search results
        n_sent = 0
        while any((self.in_progress, self.to_do, send_later)):
            self._fill_work_queues(k, n)
            try:
                query_id, meth, ret = self.result_q.get()
            except queue.Empty:
                continue
            if isinstance(ret, Exception):
                logging.warning("Hit exception while processing query %i: %s",
                                query_id, ret)
                send_later[query_id] = ret
                del self.in_progress[query_id]
                continue
            if meth == 'count_tokens':
                query = queries[query_id]
                self._scored_records(query_id, ret, query, limit)
                self.in_progress[query_id] = query
            elif meth == 'score_records':
                send_later[query_id] = ret
                del self.in_progress[query_id]
                while n_sent in send_later:
                    yield self._format_resultset(send_later.pop(n_sent))
                    logging.info("Completed query %i", n_sent)
                    n_sent += 1

    def close(self, timeout=None):
        self.backend.close()
        logging.debug("Shutting down workers")
        for i, work_q in enumerate(self.work_qs):
            work_q.put((0, 'stop', []), timeout or 0)
        for i, worker in enumerate(self.workers):
            logging.debug("Joining worker %i", i)
            worker.join()
        logging.debug("Shutdown complete")


class Index(object):
    def __init__(self, backend):
        self.backend = backend
        self.freqs = self.backend.get_freqs()
        self.rowcount = self.backend.get_rowcount()
        self.featurizer = featurizers.all[self.backend.featurizer_name]

    def _search(self, query, k=defaults.k, n=defaults.n):
        toks = [b64encode(t) for t in self.featurizer(query)]
        toks = sorted(filter(self.freqs.__contains__, toks),
                      key=self.freqs.__getitem__)[:k]
        r_map = Counter()
        for tok in toks:
            rng = self.backend.get_token(tok)
            r_map.update(rng)
        top_ids = map(first, r_map.most_common(n))
        return list(top_ids)

    def _scored_records(self, record_ids, orig_query):
        orig_features = score.features(orig_query)
        for r in self.backend.get_records(record_ids):
            s = score.hit(orig_features, score.features(r.fields))
            yield s, r

    def search(self, query, limit=defaults.limit, k=defaults.k, n=defaults.n):
        record_ids = self._search(query, k, n)
        scores_records = self._scored_records(record_ids, query)
        return [{"fields": rec.fields, "pk": rec.pk.decode(), "score": s,
                 "data": rec.data}
                for s, rec in nsmallest(limit, scores_records, key=first)]

    def close(self):
        return self.backend.close()


class CLI:

    name = "query"

    arguments = [
        (["term"], {
            "type": str,
            "nargs": "+"
        }),
        storage.backend_arg,
        (["-k", "--seeds"], {
            "type": int,
            "help": "The number of query tokens to look up in the index.",
            "default": defaults.k}),
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
