import hashlib
import re
import os
import struct
import sys
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
from scipy.integrate import quad as integrate

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


def ngrams(sequence: List[str], n: int) -> Iterable:
    """
    Code taken from NLTK, without padding.
    [Almost same as the one in utils/tokenization.py]
    Parameters
    ----------
    sequence : list
        The sequence of items to be converted into n-grams.
    n : int
        The order of the n-grams to be extracted.

    Returns
    -------
    Iterable
        The n-grams generated from the sequence.

    Examples
    --------
    >>> list(ngrams(['a', 'b', 'c', 'd'], 2))
    [('a', 'b'), ('b', 'c'), ('c', 'd')]
    >>> list(ngrams(['a', 'b', 'c', 'd'], 3))
    [('a', 'b', 'c'), ('b', 'c', 'd')]
    """
    iterables = tee(sequence, n)
    for i, sub_iterable in enumerate(iterables):
        for _ in range(i):
            next(sub_iterable, None)
    return zip(*iterables)


def sha1_hash32(data):
    """
    Directly taken from datasketch package to avoid dependency.

    Parameters
    ----------
    data : bytes

    Returns
    -------
    int
        The first 4 bytes (32 bits) of the SHA1 hash of the input data.

    Examples
    --------
    >>> sha1_hash32(b"hello")
    499578026
    >>> bin(sha1_hash32(b"hello"))
    '0b11101110001101111010010101010'
    >>> sha1_hash32(b"hello world").bit_length()
    30
    """
    return struct.unpack("<I", hashlib.sha1(data).digest()[:4])[0]


def generate_hash_values(
    content: str,
    idx: int,
    num_perm: int,
    ngram_size: int,
    hashranges: List[Tuple[int, int]],
    permutations: np.ndarray,
) -> List[Tuple[int, bytes, int]]:
    """
    Generate the MinHashLSH values for a given document.

    Parameters
    ----------
    content : str
        The content of the document.
    idx : int
        The index of the document.
    num_perm : int
        The number of permutations.
    ngram_size : int
        The size of the n-grams.
    hashranges : list
        The ranges of offsets for each hash value.
    permutations : np.ndarray
        The permutations for the hash values.

    Returns
    -------
    List[Tuple[int, bytes, int]]
        The list of (band_idx, hash value, idx) for the document.
    """
    hashvalues = np.ones(num_perm, dtype=np.uint64) * MAX_HASH
    # num tokens = len(tokens) - ngram_size + 1; number of ngram in content (consecutive)
    tokens = {" ".join(t) for t in ngrams(list(filter(lambda x: len(x) > 0, NON_ALPHA.split(content.replace('\n', ' ')))), ngram_size)}
    # (num tokens, ); convert each n-gram to hashes
    hv = np.array([sha1_hash32(token.encode("utf-8")) for token in tokens], dtype=np.uint64)
    a, b = permutations
    # (num tokens, num_perm); use each permute hash to convert hv
    phv = np.bitwise_and(((hv * np.tile(a, (len(hv), 1)).T).T + b) % MERSENNE_PRIME, MAX_HASH)
    # get minHash; int64 = 8Bytes
    hashvalues = np.vstack([phv, hashvalues]).min(axis=0)
    # split into bucket. byteswap(): Swap the bytes of the array elements
    Hs = [bytes(hashvalues[start:end].byteswap().data) for start, end in hashranges]
    return [(band_idx, H, idx) for band_idx, H in enumerate(Hs)]


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


def lines2passage(ls, spark):
    """
        lines2passage (single files)
    """
    blocks = []
    tmp = ''
    for l in ls:
        if l == '\n':
            if tmp.strip()=='':
                tmp = ''
                continue
            blocks.append(Row(text=tmp))
            tmp = ''
        else:
            tmp = tmp+l.strip('\n')+'\n' # we want to keep sentence information and we don't take it into consideration in the n-gram creatation
    try:
        pd = spark.createDataFrame(blocks)
        # pd = pd.persist(StorageLevel.MEMORY_AND_DISK)
        stat = True
    except Exception as e:
        print("Error in loading, ", e)
        pd = None
        stat = False
    return pd, stat


def unionAll(dfs):
    return reduce(DataFrame.unionAll, dfs)


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Near-deduplicating BigQuery Table with PySpark")
    parser.add_argument("--table", type=str, default=None, help="BigQuery table to deduplicate")
    parser.add_argument("--data_path", type=str, default=None, help="data_path to list of files")
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

    # TODO: what's format of bigquery?
    if args.table:
        df = spark.read.format("bigquery").option("table", args.table).load()
    elif args.data_path:
        files = os.listdir(args.data_path)
        dfs = []
        for f in tqdm(files):
            path = os.path.join(args.data_path, f)
            fio = open(path, 'r')
            lines = fio.readlines() 
            df, stat = lines2passage(lines, spark)
            if stat:
                dfs.append(df)
            else:
                print(f"Warning! Loading {f} failed.")
        df = unionAll(dfs)
        

    # add new ids with monotonically_increasing_id(to ensure un-deduplicated)
    df = df.withColumn("__id__", F.monotonically_increasing_id()).persist(StorageLevel.MEMORY_AND_DISK) # .cache()
    # Select two colums: __id__ and args.column(which we need to dedup) and Switch to rdd format. [RDD is for parallelization]
    records = df.select("__id__", args.column).rdd
    # Re-distributed rdd with args.num_perm * 2; Create a new RDD; and Persist this RDD with the default storage level (MEMORY_ONLY) using cache()
    records = records.repartition(args.num_perm * 2).persist(StorageLevel.MEMORY_AND_DISK) ##.cache()

    edges = (
        # do hash generate on every items
        # return band_idx, hash_value, idx
        records.flatMap(
            lambda x: generate_hash_values(
                content=x[1],
                idx=x[0],
                num_perm=args.num_perm,
                ngram_size=args.ngram_size,
                hashranges=HASH_RANGES,
                permutations=PERMUTATIONS,
            )
        )
        # group by band_idx&hash_value (find ones in the same band with same hash value)
        .groupBy(lambda x: (x[0], x[1]))
        # for each hash value we get edges?
        .flatMap(lambda x: generate_edges([i[2] for i in x[1]]))
        # Return a new RDD containing the distinct elements in this RDD. 
        .distinct()
        .persist(StorageLevel.MEMORY_ONLY)
        # .cache()
    )

    # TODO: What the purpose of connected component algorithm?
    a = edges
    while True:
        b = a.flatMap(large_star_map).groupByKey().flatMap(large_star_reduce).distinct().cache()
        a = b.map(small_star_map).groupByKey().flatMap(small_star_reduce).distinct().cache()
        changes = a.subtract(b).union(b.subtract(a)).collect()
        if len(changes) == 0:
            break

    results = a.collect()
    if len(results) == 0:
        log.info("No components found.")
        # pop out added new column `__id__`
        df = df.drop("__id__").cache()
        df.write.json(args.output, mode="overwrite")
        sys.exit(0)

    components = spark.createDataFrame(results, schema=["__id__", "component"]).sort(["component", "__id__"])
    components.show()
    df = df.join(components, on="__id__", how="left")
    df = df.filter(F.col("component").isNull()).drop("__id__", "component").persist(StorageLevel.MEMORY_AND_DISK) # cache()
    # TODO: write out to txt and control the size? saveAsTextFile
    # df.write.json(args.output, mode="overwrite")
    df.write.format("json").mode("overwrite").save(args.output)
