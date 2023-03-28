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
    # split into bucket. byteswap(): Swap the bytes of the array elements;
    # e.g. hashvalues = np.asarray([11, 34], dtype=np.uint64)
    # bytes(hashvalues) -> b'\x0b\x00\x00\x00\x00\x00\x00\x00"\x00\x00\x00\x00\x00\x00\x00'
    # bytes(hashvalues.byteswap().data) -> b'\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00"'
    Hs = [base64.b64encode(bytes(hashvalues[start:end].byteswap().data)).decode('utf-8') for start, end in hashranges]
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
    parser.add_argument("--output", "-d", type=str, required=True, help="Duplicated ids directory")
    parser.add_argument("--rm_ori", action="store_true", help="Remove original files")
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


    # we should deal with it one-by-one
    split_files = os.listdir(args.data_path.strip('\n')+'_tmp_withid')
    split_files.sort()
    assert len(split_files) == 1
    data_path = split_files[0]
    schema = StructType([
        StructField("__idconsec__", IntegerType(),True),
        StructField("text", StringType(),True)
    ])
    df = spark.read.schema(schema).json("file://{}".format(args.data_path.strip('\n')+'_tmp_withid/'+data_path))
    # https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/
    # Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for that record and drops records from right where match not found.


    df = df.rdd
    # Re-distributed rdd with args.num_perm * 2; Create a new RDD; and Persist this RDD with the default storage level (MEMORY_ONLY) using cache()
    df = df.repartition(args.num_perm * 2).cache()
    # do hash generate on every items
    # return band_idx, hash_value, idx
    df = df.flatMap(
        lambda x: generate_hash_values(
            content=x[1],
            idx=x[0],
            num_perm=args.num_perm,
            ngram_size=args.ngram_size,
            hashranges=HASH_RANGES,
            permutations=PERMUTATIONS,
        )
    )

    df.toDF().write.format("csv").mode("overwrite").save(args.output)