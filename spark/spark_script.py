import json
import argparse
from base64 import b64encode

from pyspark import SparkConf, SparkContext

from polymr.index import features
from polymr.util import merge_to_range


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Input dataset to index")
    parser.add_argument("-o", "--output", help="Output data")
    parser.add_argument("-s", "--field-separator", default="|",
                        help=("String that separates one field "
                              "from another in input"))
    parser.add_argument("-f", "--fields",
                        help=("Comma separated list of field idices "
                              "to index, the last field should be the "
                              "object's numeric primary key"))
    return parser.parse_args()


if __name__ == '__main__':

    args = cli()
    indexed_fields = list(map(int, args.fields.split(",")))

    spark = SparkContext(
        conf=(SparkConf()
              .setAppName('polymr_index')
              .config('spark.debug.maxToStringFields', 1000))
    )

    def _deserialize(line):
        fields = line.split(args.field_separator)
        if len(fields) < max(indexed_fields):
            return []
        return [ fields[i] for i in indexed_fields ]

    def _featurize(fields):
        pk = fields[-1]
        fs = features(fields[:-1])
        return [ (b64encode(f).decode(), pk) for f in fs ]

    (spark.textFile(args.input)
     .top(1000)
     .map(_deserialize)
     .flatMap(_featurize)
     .groupByKey()
     .map(lambda a: (a[0], json.dumps(merge_to_range(sorted(a[1])))))
     .saveAsTextFile(
         args.output,
         compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec'))
     
    spark.stop()
    


