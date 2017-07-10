import csv
from itertools import chain
from collections import namedtuple

from toolz import get


class Record(namedtuple("Record", ["fields", "pk", "data"])):
    """Indexing organizes records for easy lookup and searching finds the
    closest record to a query. This class defines what a record is.

    :param fields: The attributes used to find a record. Searchers
      will supply something like these to find records and the indexer
      will use these to organize the records for easy lookup
    :type fields: tuple of str

    :param pk: The primary key used to find this record in other
      databases.
    :type pk: str

    :param data: The attributes not used to find a record, but you
      want to store anyway.
    :type pk: tuple of str

    """
    pass


def _from_general(rows, searched_fields_idxs=None, pk_field_idx=None,
                  include_data=True):
    a = next(rows)
    allidxs = list(range(len(a)))
    if searched_fields_idxs is None:
        searched_fields_idxs = allidxs[:-1]
    if pk_field_idx is None:
        pk_field_idx = allidxs[-1]
    elif pk_field_idx < 0:
        pk_field_idx = allidxs[pk_field_idx]
    data_idxs = [i for i in allidxs
                 if i not in set(chain([pk_field_idx], searched_fields_idxs))]

    if include_data:
        def _make(row):
            return Record(tuple(get(searched_fields_idxs, row, "")),
                          get(pk_field_idx, row, ""),
                          tuple(get(data_idxs, row, "")))
    else:
        def _make(row):
            return Record(tuple(get(searched_fields_idxs, row, "")),
                          get(pk_field_idx, row, ""),
                          tuple())

    return map(_make, chain([a], rows))


def from_csv(f, searched_fields_idxs=None, pk_field_idx=None,
             include_data=True):
    rows = csv.reader(f)
    return _from_general(rows, searched_fields_idxs,
                         pk_field_idx, include_data)


def from_psv(f, searched_fields_idxs=None, pk_field_idx=None,
             include_data=True):
    def _rows():
        for line in f:
            row = line.strip()
            if row:
                yield row.split('|')

    return _from_general(_rows(), searched_fields_idxs,
                         pk_field_idx, include_data)


readers = dict(csv=from_csv,
               psv=from_psv)
