"""
spark-submit --executor-memory=50G --driver-memory=4G --deploy-mode=client --executor-cores=64 rmdup.py   --data_path "/tmp/code/tiny_owt" --dedup_ids "/tmp/code/dedup_ids"  --column "text"   --ngram 13   --num_perm 10   --threshold 0.7
spark-submit --executor-memory=50G --driver-memory=4G --deploy-mode=client --executor-cores=64 rmdup.py   --data_path "/tmp/code/tiny_owt2" --dedup_ids "/tmp/code/dedup_ids"  --column "text"   --ngram 13   --num_perm 10   --threshold 0.7
"""
import hashlib
import re
import os
import struct
import sys
import base64
import time
from tqdm import tqdm
from itertools import tee
from logging import Logger
from typing import Iterable
from typing import List
from typing import Tuple

import numpy as np
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructType, IntegerType, StructField
from scipy.integrate import quad as integrate

import gc
from memory_profiler import profile
from functools import reduce  # For Python 3.x

SEED = 42
NON_ALPHA = re.compile("[^A-Za-z_0-9]")
RNG = np.random.RandomState(SEED)
MAX_HASH = np.uint64((1 << 32) - 1)
MERSENNE_PRIME = np.uint64((1 << 61) - 1)


# Connected Components in MapReduce and Beyond
def large_star_map(edge):
    return [(edge[0], edge[1]), (edge[1], edge[0])]


def large_star_reduce(group):
    x, neighbors = group
    nodes = [x] + list(neighbors)
    minimum = min(nodes)
    return [(n, minimum) for n in nodes if n > x]


def small_star_map(edge):
    x, y = edge
    if y <= x:
        return (x, y)
    else:
        return (y, x)


def small_star_reduce(group):
    x, neighbors = group
    nodes = [x] + list(neighbors)
    minimum = min(nodes)
    return [(n, minimum) for n in nodes if n != minimum]


def optimal_param(
    threshold: float,
    num_perm: int,
    false_positive_weight: float = 0.5,
    false_negative_weight: float = 0.5,
):
    """
    Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
    of probabilities of false positive and false negative, taken from datasketch.

    Parameters
    ----------
    threshold : float
        The threshold for similarity.
    num_perm : int
        The number of permutations.
    false_positive_weight : float
        The weight of false positive.
    false_negative_weight : float
        The weight of false negative.

    Returns
    -------
    Tuple[int, int]
        The optimal `b` and `r` parameters.
        The number of bands, and the number of rows per band respectively.

    Examples
    --------
    >>> optimal_param(0.7, 256)
    (25, 10)
    """

    def false_positive_probability(threshold: float, b: int, r: int):
        """Source: `datasketch.lsh`"""

        def proba(s):
            return 1 - (1 - s ** float(r)) ** float(b)

        a, _ = integrate(proba, 0.0, threshold)
        return a

    def false_negative_probability(threshold: float, b: int, r: int):
        """Source: `datasketch.lsh`"""

        def proba(s):
            return 1 - (1 - (1 - s ** float(r)) ** float(b))

        a, _ = integrate(proba, threshold, 1.0)
        return a

    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, num_perm + 1):
        max_r = int(num_perm / b)
        for r in range(1, max_r + 1):
            fp = false_positive_probability(threshold, b, r)
            fn = false_negative_probability(threshold, b, r)
            error = fp * false_positive_weight + fn * false_negative_weight
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt


def unionAll(dfs):
    return reduce(DataFrame.unionAll, dfs)


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Near-deduplicating BigQuery Table with PySpark")
    parser.add_argument("--data_path", type=str, default=None, help="file to list of data_path")
    parser.add_argument("--threshold", type=float, default=0.7, help="Similarity threshold")
    parser.add_argument("--ngram_size", type=int, default=5, help="N-gram size")
    parser.add_argument("--num_perm", type=int, default=256, help="Number of permutations")
    parser.add_argument("--b", type=int, default=None, help="Number of bands")
    parser.add_argument("--r", type=int, default=None, help="Number of rows per band")
    parser.add_argument("--column", "-c", type=str, default="content", help="Column to deduplicate")
    parser.add_argument("--dedup_ids", "-d", type=str, required=True, help="Duplicated ids directory")
    args = parser.parse_args()

    conf = SparkConf()
    conf.set("spark.app.name", "MinHashLSH")
    conf.set("spark.sql.debug.maxToStringFields", "100")
    # conf.set("spark.sql.files.maxRecordsPerFile", "0")  # no-limited save rows to files; Maximum number of records to write out to a single file. If this value is zero or negative, there is no limit.
    # conf.set("spark.sql.shuffle.partitions", "200")  # it seems to control the output file numbers, since we don't gather them, so the number is equal to the partitions. # The default number of partitions to use when shuffling data for joins or aggregations. Note: For structured streaming, this configuration cannot be changed between query restarts from the same checkpoint location.
    # conf.set('spark.executor.memory','300g')  # 50g
    # conf.set('spark.driver.memory','8g')      # 8g
    conf.set('spark.executor.cores','64')
    # conf.set('spark.driver.maxResultsSize','0')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log: Logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # type: ignore

    if args.b is None or args.r is None:
        B, R = optimal_param(args.threshold, args.num_perm)
        log.info(f"Using optimal parameters: {B}, {R}")
    else:
        B, R = args.b, args.r
        _B, _R = optimal_param(args.threshold, args.num_perm)
        log.info(f"Using parameters: {B}, {R}, with optimal parameters: {_B}, {_R}")

    HASH_RANGES = [(i * R, (i + 1) * R) for i in range(B)]
    PERMUTATIONS = np.array(
        [
            (
                RNG.randint(1, MERSENNE_PRIME, dtype=np.uint64),
                RNG.randint(0, MERSENNE_PRIME, dtype=np.uint64),
            )
            for _ in range(args.num_perm)
        ],
        dtype=np.uint64,
    ).T


    # load duplicate ids
    components = spark.read.option("delimiter", ",").csv(args.dedup_ids)
    # columns: _c0, _c1 : __idconsec__, appear times
    components = components.withColumn("_c0", components['_c0'].cast(IntegerType()))
    components = components.withColumn("_c1", components['_c1'].cast(IntegerType()))
    components = components.withColumnRenamed("_c0","__idconsec__")
    components = components.withColumnRenamed("_c1","component")

    # we should deal with it one-by-one
    split_files = os.listdir(args.data_path.strip('\n')+'_tmp_withid')
    split_files.sort()
    for data_path in split_files:
        schema = StructType([
            StructField("__idconsec__", IntegerType(),True),
            StructField("text", StringType(),True)
        ])
        df = spark.read.schema(schema).json("file://{}".format(args.data_path.strip('\n')+'_tmp_withid/'+data_path))\
        # https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/
        # Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for that record and drops records from right where match not found.
        df = df.join(components, on="__idconsec__", how="left")
        before_count = df.count()

        # So: F.col("component").isNull() means: appear in original df, but not exist in components;
        # we only keep these rows. (don't find in any components)
        # TODO: why not left one for each component??
        df = df.filter(F.col("component").isNull()).drop("__idconsec__", "component").persist(StorageLevel.MEMORY_AND_DISK) # cache()
        after_count = df.count()
        print(" For file {}: before / after: {} / {}".format(args.data_path.strip('\n')+'/'+data_path, before_count, after_count))
        df.write.format("json").mode("overwrite").save(args.data_path.strip('\n')+'_dedup/'+data_path)
