"""
spark-submit --executor-memory=50G --driver-memory=1500M --deploy-mode=client --executor-cores=64 loadhash.py   --data_path_file "data_paths.txt"   --output "/tmp/code/dedup_ids"   --column "text"   --ngram 13   --num_perm 10   --threshold 0.7
what's in data_paths.txt:
    - /tmp/code/tiny_owt
    - /tmp/code/tiny_owt2
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
from pyspark.sql.types import StringType, StructType, IntegerType, StructField, BinaryType
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


def generate_edges(nodes: List[int]) -> List[Tuple[int, int]]:
    """
    Generate edges from a cluster. Instead of generating N^2 edges, we only need all nodes align to a single node, since
    we will be running connected components on the edges later.

    Parameters
    ----------
    nodes : List[int]
        The list of nodes in the cluster.

    Returns
    -------
    List[Tuple[int, int]]
        The list of edges.
    """
    if len(nodes) <= 1:
        return []

    min_node = min(nodes)
    return [(n, min_node) for n in nodes if n != min_node]


def unionAll(dfs):
    return reduce(DataFrame.unionAll, dfs)


def decode_hash(x):
    return [(x[0], bytes(base64.b64decode(x[1].encode('utf-8'))), x[2])]

# https://pypi.org/project/memory-profiler/
# https://www.databricks.com/blog/2022/11/30/memory-profiling-pyspark.html
@profile
def load_hash(data_path):
    # if we want to save hashes; [but we are still facing loading b64decode problem; maybe the utf-8 encoding by-default in csv dumping?]
    # try: records.toDF().write.format("csv").mode("overwrite").save(args.output)
    # and in generate_hash: use base64.b64encode(bytes(hashvalues[start:end].byteswap().data))
    # load and decode:
    # !!: BinaryType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
    # !!: BinaryType: Represents byte sequence values.
    # columns: _c0, _c1, _c2
    records = spark.read.option("delimiter", ",").csv(data_path)
    ## V1
    # udf = F.UserDefinedFunction(lambda x: base64.b64decode(x.encode('utf-8')), BinaryType())
    # records = records.withColumn("_c1", udf(records['_c1']))
    ## V2
    records = records.withColumn("_c1", records['_c1'].cast(StringType()))
    records = records.withColumn("_c0", records['_c0'].cast(IntegerType()))
    records = records.withColumn("_c2", records['_c2'].cast(IntegerType()))
    records = records.rdd.cache()
    ## V2
    records = records.flatMap(
        lambda x: decode_hash(
            x=x,
        )
    )
    ## V3
    # records = records.rdd.flatMap(lambda x: [x[0], base64.b64decode(x[1].encode('utf-8')), x[2]]).cache()
    return records


@profile
def create_edges(records):
    edges = (
        # group by band_idx&hash_value (find ones in the same band with same hash value)
        records.groupBy(lambda x: (x[0], x[1]))        
        # for each hash value we get edges?
        .flatMap(lambda x: generate_edges([i[2] for i in x[1]]))
        # Return a new RDD containing the distinct elements in this RDD. 
        .distinct()
        .persist(StorageLevel.MEMORY_ONLY)
        # .cache()
    )
    return edges


@profile
def find_components(a):
    # a = edges
    while True:
        b = a.flatMap(large_star_map).groupByKey().flatMap(large_star_reduce).distinct().cache()
        a = b.map(small_star_map).groupByKey().flatMap(small_star_reduce).distinct().cache()
        changes = a.subtract(b).union(b.subtract(a)).collect()
        if len(changes) == 0:
            break
    b.unpersist()
    del b
    gc.collect()
    return a


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Near-deduplicating BigQuery Table with PySpark")
    parser.add_argument("--data_path_file", type=str, default=None, help="file to list of data_path")
    parser.add_argument("--threshold", type=float, default=0.7, help="Similarity threshold")
    parser.add_argument("--ngram_size", type=int, default=5, help="N-gram size")
    parser.add_argument("--num_perm", type=int, default=256, help="Number of permutations")
    parser.add_argument("--b", type=int, default=None, help="Number of bands")
    parser.add_argument("--r", type=int, default=None, help="Number of rows per band")
    parser.add_argument("--column", "-c", type=str, default="content", help="Column to deduplicate")
    parser.add_argument("--output", "-o", type=str, required=True, help="Output directory")
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

    data_paths = open(args.data_path_file).readlines()
    records = None
    for data_path in tqdm(data_paths):
        # https://stackoverflow.com/questions/27299923/how-to-load-local-file-in-sc-textfile-instead-of-hdfs
        _records = load_hash("file://{}".format(data_path.strip('\n')+'_hashes'))
        if records:
            records = records.union(_records)
        else:
            records = _records
    # debug only: to force compute and test memory
    # records.toDF().show(1)
    print("total items found: ", records.count())
    time.sleep(10)

    edges = create_edges(records)
    results = find_components(edges).collect()
    edges.unpersist()
    del edges
    gc.collect()

    if len(results) == 0:
        log.info("No components found.")
        # pop out added new column `__id__`
        df = results.drop("__id__").cache()
        df.write.json(args.output, mode="overwrite")
        sys.exit(0)

    # componet means how many times occured beyond once
    components = spark.createDataFrame(results, schema=["__idconsec__", "component"]).sort(["component", "__idconsec__"])
    components.show(1)
    print("duplicate items found: ", components.count())
    time.sleep(10)
    components = components.rdd.repartition(args.num_perm * 2)
    components.toDF().write.format("csv").mode("overwrite").save(args.output)


    # we should deal with it one-by-one
    """
    for folder in tqdm(data_paths):
        split_files = os.listdir(folder.strip('\n')+'_tmp_withid')
        for data_path in split_files:
            schema = StructType([
                StructField("text", StringType(),True),
                StructField("__id__", IntegerType(),True),
                StructField("__idconsec__", IntegerType(),True),
            ])
            df = spark.read.schema(schema).json("file://{}".format(folder.strip('\n')+'_tmp_withid/'+data_path)).select("text", "__idconsec__")
            # https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/
            # Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for that record and drops records from right where match not found.
            df = df.join(components, on="__idconsec__", how="left")

            # So: F.col("component").isNull() means: appear in original df, but not exist in components;
            # we only keep these rows. (don't find in any components)
            # TODO: why not left one for each component??
            df = df.filter(F.col("component").isNull()).drop("__idconsec__", "component").persist(StorageLevel.MEMORY_AND_DISK) # cache()
            df.write.format("json").mode("overwrite").save(folder.strip('\n')+'_dedup/'+data_path)
    """