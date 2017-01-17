import json
from heapq import nsmallest
from base64 import b64encode
from collections import Counter
from operator import itemgetter

from . import index
from . import score
from . import storage

first = itemgetter(0)

class defaults:
    n = 600
    k = 12
    limit = 5

class Index(object):
    def __init__(self, backend):
        self.backend = backend
        self.freqs = self.backend.get_freqs()
        self.rowcount = self.backend.get_rowcount()


    def _search(self, query, k=defaults.k, n=defaults.n):
        toks = [ b64encode(t).decode() for t in index.features(query) ]
        toks = sorted(filter(self.freqs.__contains__, toks),
                      key=lambda t: self.freqs.get(t, 0))[:k]
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
        return [ { "fields": rec.fields, "pk": rec.pk, "score": s }
                 for s, rec in nsmallest(limit, scores_records, key=first) ]


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
        print( json.dumps(results, indent=2) )
