import os
import sys
import logging
import multiprocessing
import contextlib
from tempfile import NamedTemporaryFile
from gzip import GzipFile as CompressedFile
from heapq import merge as _merge
from base64 import b64encode
from itertools import groupby
from itertools import repeat
from collections import defaultdict
from operator import itemgetter

from toolz import partition_all

from . import storage
from . import record
from . import util
from . import featurizers

fst = itemgetter(0)
snd = itemgetter(1)

logger = logging.getLogger(__name__)


def _ef_worker(args):
    chunk, featurizer_name = args
    features = featurizers.all[featurizer_name]
    ksets = [(i, features(rec.fields)) for i, rec in chunk]
    d = defaultdict(list)
    for i, kset in ksets:
        for kmer in kset:
            d[kmer].append(i)
    kmer_is = [(b64encode(kmer).decode(), ",".join(map(str, sorted(rset))))
               for kmer, rset in d.items()]
    tmpfile = NamedTemporaryFile(dir=".", suffix="polymr_tmp_chunk.txt.gz",
                                 delete=False)
    fname = tmpfile.name
    with CompressedFile(fileobj=tmpfile, mode='w') as f:
        for kmer, ids in sorted(kmer_is):
            data = "|".join((kmer, ids))+"\n"
            f.write(data.encode())
    return fname


def _initializer(tmpdir):
    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)
    os.chdir(tmpdir)


def _tmpparse_split(fobj):
    for line in fobj:
        kmer, ids = line.strip().split(b"|")
        ids = list(map(int, ids.decode().split(',')))
        yield kmer, ids


def _tmpparse(fobj):
    for line in fobj:
        kmer, ids = line.strip().split(b"|")
        yield kmer, ids


def _merge_tmpfiles(fnames):
    tmpout = NamedTemporaryFile(dir='.', suffix="polymr_tmp_chunk.txt.gz",
                                delete=False)
    with contextlib.ExitStack() as stack:
        fileobjs = [stack.enter_context(CompressedFile(fname, 'r'))
                    for fname in fnames]
        with CompressedFile(fileobj=tmpout, mode='w') as outf:
            kmer_ids = _merge(*map(_tmpparse, fileobjs), key=fst)
            for kmer, ids in kmer_ids:
                outf.write(b"|".join((kmer, ids))+b"\n")
    for fname in fnames:
        os.remove(fname)
    return tmpout.name


def _mergefeatures(tmpnames):
    with contextlib.ExitStack() as stack:
        fileobjs = [stack.enter_context(CompressedFile(fname, 'r'))
                    for fname in tmpnames]
        kmer_ids = _merge(*map(_tmpparse_split, fileobjs), key=fst)
        for kmer, kmer_chunks in groupby(kmer_ids, key=fst):
            rng, compacted = util.merge_to_range(map(snd, kmer_chunks))
            yield kmer, rng, compacted


def extract_features(input_records, nproc, chunksize, backend,
                     tmpdir="/tmp", featurizer_name='default'):
    pool = multiprocessing.Pool(nproc, _initializer, (tmpdir,))
    chunks = partition_all(chunksize, enumerate(input_records))
    tmpnames = pool.imap_unordered(_ef_worker,
                                   zip(chunks, repeat(featurizer_name)),
                                   chunksize=1)
    tmpnames = list(tmpnames)
    tmpchunks = partition_all(len(tmpnames)//nproc + 1, tmpnames)
    tmpnames = pool.imap_unordered(_merge_tmpfiles, tmpchunks, chunksize=1)
    tmpnames = list(tmpnames)
    tokens = _mergefeatures(tmpnames)
    tokfreqs = {}
    for token, record_ids, compacted in tokens:
        tokfreqs[token] = sum(x[1] - x[0] + 1 if type(x) is list else 1
                              for x in record_ids)
        backend.save_token(token, record_ids, compacted)
    backend.save_freqs(tokfreqs)
    for tmpname in tmpnames:
        os.remove(tmpname)


def records(input_records, backend):
    rowcount = backend.save_records(enumerate(input_records))
    backend.save_rowcount(rowcount)


class CLI:

    name = "index"

    arguments = [
        (["-1", "--step1"], {
            "type": bool,
            "help": "Step 1: Generate and index features"
        }),
        storage.backend_arg,
        (["-2", "--step2"], {
            "type": bool,
            "help": "Store non-search attributes"
        }),
        (["-i", "--input"], {
            "help": "Defaults to stdin"
        }),
        (["-n", "--parallel"], {
            "type": int,
            "default": 1,
            "help": "Number of concurrent workers"
        }),
        (["--primary-key"], {
            "type": int,
            "default": -1,
            "help": "Base 0 index of primary key in input data"}),
        (["--search-idxs"], {
            "type": str,
            "help": ("Comma separated list of base 0 indices of "
                     "attributes to be used when looking up an "
                     "indexed object.")}),
        (["--tmpdir"], {
            "help": "Where to store temporary files",
            "default": "/tmp"
        }),
        (["--chunksize"], {
            "help": "Number of records for each worker to process in memory",
            "type": int,
            "default": 50000
        }),
        (["--featurizer"], {
            "help": "The featurizer to use when indexing records",
            "default": 'default',
            "choices": featurizers.all
        }),
    ]

    @staticmethod
    def hook(parser, args):
        try:
            sidxs = list(map(int, args.search_idxs.split(",")))
        except AttributeError:
            print("Error parsing --search-idxs", file=sys.stderr)
            parser.print_help()
            sys.exit(1)

        if args.step1:
            backend = storage.parse_url(args.backend)
            with util.openfile(args.input or sys.stdin) as inp:
                recs = record.from_csv(
                    inp,
                    searched_fields_idxs=sidxs,
                    pk_field_idx=args.primary_key,
                    include_data=False
                )
                ret = extract_features(recs, args.parallel, args.chunksize,
                                       backend, tmpdir=args.tmpdir,
                                       featurizer_name=args.featurizer)
            return ret
        elif args.step2:
            backend = storage.parse_url(args.backend)
            with util.openfile(args.input or sys.stdin) as inp:
                recs = record.from_csv(
                    inp,
                    searched_fields_idxs=sidxs,
                    pk_field_idx=args.primary_key,
                )
                ret = records(recs, backend)
            return ret
        else:
            return parser.print_help()
