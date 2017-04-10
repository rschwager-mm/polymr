from .util import avg
from .util import jaccard
from .util import ngrams


def features(record):
    return [set(ngrams(attr, k=2, step=1)) for attr in record]


def hit(query_features, result_features):
    return avg(list(map(jaccard, query_features, result_features)))
