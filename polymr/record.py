import csv
from collections import namedtuple

class Record(namedtuple("Record", ["fields", "pk"])):
    """Indexing organizes records for easy lookup and searching finds the
    closest record to a query. This class defines what a record is.

    :param fields: The attributes used to find a record. Searchers
      will supply something like these to find records and the indexer
      will use these to organize the records for easy lookup
    :type fields: list of str

    :param pk: The primary key used to find this record in other
      databases.
    :type pk: str

    """
    pass


def from_csv(f):
    for row in csv.reader(f):
        yield Record(row[:-1], row[-1])
