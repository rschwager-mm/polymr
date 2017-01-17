from heapq import merge as _merge

KMER_SIZE=3
STEP_SIZE=1


def get(keys, idxable):
    return [idxable[k] for k in keys]


def avg(l):
    return sum(l)/len(l)


def ngrams(s, k=KMER_SIZE, step=STEP_SIZE):
    if len(s) < k:
        return [s]
    return [s[i:i+k] for i in range(0, len(s)-k+1, step)]


def jaccard(a, b):
    if not a and not b:
        return 0
    n = len(a.intersection(b))
    return 1 - ( float(n) / (len(a) + len(b) - n) )


def openfile(filename_or_handle, mode='r'):
    if type(filename_or_handle) is str:
        return open(filename_or_handle, mode)
    return filename_or_handle


def merge_to_range(ls):
    if len(ls) == 1:
        merged = iter(ls[0])
    else:
        merged = _merge(*ls)
    ret = [next(merged)]
    prev = ret[0]
    compacted = False
    for x in merged:
        if x == prev + 1:
            if type(ret[-1]) is list:
                ret[-1][-1] = x
            else:
                ret[-1] = [prev, x]
                compacted = True
        else:
            ret.append(x)
        prev = x
    return ret, compacted
            

