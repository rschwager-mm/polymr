from zlib import compress as _compress

from . import util


def featurize_compress(rec):
    fs = set()
    for attr in rec:
        fs.update(util.ngrams(_compress(attr.encode())))
    return fs


def featurize_k3(rec):
    fs = set()
    for attr in rec:
        fs.update(util.ngrams(attr.encode()))
    return fs


def featurize_k2(rec):
    fs = set()
    for attr in rec:
        fs.update(util.ngrams(attr.encode(), k=2, step=1))
    return fs


all = dict(k3=featurize_k3,
           k2=featurize_k2,
           compress=featurize_compress,
           default=featurize_compress)
