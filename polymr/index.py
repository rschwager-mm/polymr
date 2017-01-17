import sys
import json
import logging
import multiprocessing
from zlib import compress as _compress
from base64 import b64encode
from collections import defaultdict

from toolz import partition_all

from . import storage
from . import record
from . import util


logger = logging.getLogger(__name__)


def features(rec):
    fs = set()
    for attr in rec:
        fs.update( util.ngrams(_compress(attr.encode())) )
    return fs


def _ef_worker(chunk):
    d = defaultdict(list)
    ksets = [ (i, features(rec.fields)) for i, rec in chunk ]
    for i, kset in ksets:
        for kmer in kset:
            d[kmer].append(i)
    return { b64encode(kmer).decode() : rset
             for kmer, rset in d.items() }

        
def extract_features(input_file, nproc, chunksize, output_file=sys.stdout):
    pool = multiprocessing.Pool(nproc)
    chunks = partition_all(chunksize, enumerate(record.from_csv(input_file)))
    kmer_tables = pool.imap_unordered(_ef_worker, chunks, chunksize=1)
    for i, kmer_table in enumerate(kmer_tables):
        print(json.dumps(kmer_table), file=output_file)
        logging.info("processed %i records", i*chunksize)


def unpack(input_file, backend):
    alltoks = set()
    for i, tab in enumerate(map(json.loads, input_file)):
        alltoks.update(tab.keys())
        backend.save_tokprefix([ (tok, i, rng) for tok, rng in tab.items() ])
        logging.info("Processed %s tables", i)
    backend.save_toklist(list(alltoks))
    logging.info("Saved %s tokens", len(alltoks))


def organize(backend):
    alltoks = backend.get_toklist()
    tokfreqs = {}
    for tok in alltoks:
        keys, rngs = backend.get_tokprefix(tok)
        if not keys:
            continue
        logging.info("Merging tokens `%s' to `%s'", keys[0], keys[-1])
        rng, compacted = util.merge_to_range(rngs)
        backend.save_token(tok, rng, compacted)
        tokfreqs[tok] = sum(x[1]-x[0]+1 if type(x) is list else 1 for x in rng)
        backend.delete_tokprefix(tok)
        logging.info("Merged ranges for token `%s'", tok)
    backend.save_freqs(tokfreqs)


def records(input_file, backend):
    rs = record.from_csv(input_file)
    rowcount = backend.save_records(enumerate(rs))
    backend.save_rowcount(rowcount)



class CLI:

    name = "index"

    arguments = [
        (["-1", "--step1"], {
            "type": bool,
            "help": "Step 1: Generate feature sets from records"
        }),
        (["-2", "--step2"], {
            "type": bool,
            "help": "Step 2: Unpack feature sets into storage backend"
        }),
        (["-3", "--step3"], {
            "type": bool,
            "help": ("Step 3: Organize unpacked feature sets within storage"
                     " backend")
        }),
        storage.backend_arg,
        (["-4", "--step4"], {
            "type": bool,
            "help": "Store primary keys and indexed attributes"
        }),
        (["-i", "--input"], {
            "help": "Defaults to stdin"
        }),
        (["-n", "--parallel"], {
            "type": int,
            "default": 1,
            "help": "Number of concurrent workers"
        }),
        (["--chunksize"], {
            "help": "Number of records for each worker to process in memory",
            "type": int,
            "default": 50000
        }),
    ]

    @staticmethod
    def hook(parser, args):
        if args.step1:
            with util.openfile(args.input or sys.stdin) as inp:
                ret = extract_features(inp, args.parallel, args.chunksize)
            return ret
        elif args.step2:
            backend = storage.parse_url(args.backend)
            with util.openfile(args.input or sys.stdin) as inp:
                ret = unpack(inp, backend)
            return ret
        elif args.step3:
            return organize(storage.parse_url(args.backend))
        elif args.step4:
            backend = storage.parse_url(args.backend)
            with util.openfile(args.input or sys.stdin) as inp:
                ret = records(inp, backend)
            return ret
        else:
            return parser.print_help()
        

