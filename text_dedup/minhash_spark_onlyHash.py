"""
spark-submit --executor-memory=50G --driver-memory=4G --deploy-mode=client --executor-cores=64 hashonly.py   --data_path "/tmp/code/tiny_owt"   --output "/tmp/code/tiny_owt_hashes"   --column "text"   --ngram 13   --num_perm 10   --threshold 0.7
spark-submit --executor-memory=50G --driver-memory=4G --deploy-mode=client --executor-cores=64 hashonly.py   --data_path "/tmp/code/tiny_owt2"   --output "/tmp/code/tiny_owt2_hashes"   --column "text"   --ngram 13   --num_perm 10   --threshold 0.7
"""
import hashlib
import re
import os
import struct
import sys
import base64
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
            # blocks.append(Row(text=tmp))
            blocks.append((tmp,))
            tmp = ''
        else:
            tmp = tmp+l.strip('\n')+'\n' # we want to keep sentence information and we don't take it into consideration in the n-gram creatation
    try:
        # pd = spark.createDataFrame(blocks)
        pd = spark.createDataFrame(blocks, schema='text string')
        # pd = pd.persist(StorageLevel.MEMORY_AND_DISK)
        stat = True
    except Exception as e:
        print("Error in loading, ", e)
        pd = None
        stat = False
    del blocks
    gc.collect()
    return pd, stat


def unionAll(dfs):
    return reduce(DataFrame.unionAll, dfs)


def decode_hash(x):
    return [(x[0], bytes(base64.b64decode(x[1].encode('utf-8'))), x[2])]
    
# @profile
def load_hash(data_path):
    # if we want to save hashes; [but we are still facing loading b64decode problem; maybe the utf-8 encoding by-default in csv dumping?]
    # try: records.toDF().write.format("csv").mode("overwrite").save(args.output)
    # and in generate_hash: use base64.b64encode(bytes(hashvalues[start:end].byteswap().data))
    # load and decode:
    # !!: ByteType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
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


# https://pypi.org/project/memory-profiler/
# https://www.databricks.com/blog/2022/11/30/memory-profiling-pyspark.html
# @profile
def load_and_hash(files, args):
    records = None
    if os.path.exists('PREV_ID'):
        previous_max_value = int(open('PREV_ID').read())
    else:
        previous_max_value = 0
    files.sort()
    for f in tqdm(files):
        path = os.path.join(args.data_path, f)
        fio = open(path, 'r')
        lines = fio.readlines() 
        if args.rm_ori:
            print("Warning! removing original file: {}".format(path))
            os.system("rm {}".format(path))
        df, stat = lines2passage(lines, spark)
        # lines = [base64.b64decode(l).decode('utf-8') for l in lines] # we run convertTxt first
        # try:
        #     df = spark.createDataFrame(lines, schema='text string')
        #     # pd = pd.persist(StorageLevel.MEMORY_AND_DISK)
        #     stat = True
        # except Exception as e:
        #     print("Error in loading, ", e)
        #     df = None
        #     stat = False
        del lines
        if stat:
            # add new ids with monotonically_increasing_id(to ensure un-deduplicated)
            df = df.withColumn("__id__", F.monotonically_increasing_id()).cache()
            # https://kb.databricks.com/en_US/sql/gen-unique-increasing-values
            # TODO: do this with partition.
            window = Window.orderBy(F.col('__id__'))
            # begin with row_number = 1
            # Select two colums: __idconsec__ and args.column(which we need to dedup) and Switch to rdd format. [RDD is for parallelization]
            df = df.withColumn("__idconsec__", F.row_number().over(window) + F.lit(previous_max_value)).select("__idconsec__", args.column)
            # save to disk for later read in
            df.write.format("json").mode("overwrite").save(args.data_path+'_tmp_withid/'+f)
            # os.rmtree(path)
            # get the next beginning
            previous_max_value = df.agg({"__idconsec__": "max"}).first()[0]
            print(previous_max_value)
            
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
            if records:
                records = records.union(df)
            else:
                records = df
            # https://stackoverflow.com/a/39967109/11821194
            # actully del df + gc.collect() will unpersist the rdd directly
            df.unpersist()
            del df
            gc.collect()
            # deptColumns = ["bucket_id","hashes","__idconsec__"]
            # print(records.toDF(deptColumns).agg({"__idconsec__": "max"}).first()[0])
        else:
            print(f"Warning! Loading {f} failed.")
        # if we want to save hashes; [but we are still facing loading b64decode problem; maybe the utf-8 encoding by-default in csv dumping?]
        # try: records.toDF().write.format("csv").mode("overwrite").save(args.output)
        # and in generate_hash: use base64.b64encode(bytes(hashvalues[start:end].byteswap().data)).decode('utf-8')
        # load and decode:
        # df = spark.read.option("delimiter", ",").csv(args.data_path).rdd
        # df = df.flatMap(lambda x: [x[0], base64.b64decode(x[1].encode('utf-8')), x[2]]).cache()
    
    f = open('PREV_ID', 'w') 
    f.write(f'{previous_max_value}')
    records.toDF().write.format("csv").mode("overwrite").save(args.output)

    """FOR DEBUG on Loading"""
    # df = records.toDF()
    # print(df.schema["_2"].dataType)
    # BinaryType()
    # for col in df.dtypes:
    #     print(col[0]+" , "+col[1])
    # _1 , bigint
    # _2 , binary
    # _3 , bigint
    
    # # saved hashes
    # saved_records = load_hash("file://{}".format('/tmp/code/tiny_owt_hashes'))
    # print("total items found (loaded one): ", saved_records.count())
    # print("total items found (online one): ", records.count())

    # changes = saved_records.subtract(records).union(records.subtract(saved_records)).collect()
    # print("Differences: ", len(changes))

    # schema = StructType([
    #     StructField("bucket_id", IntegerType(),True),
    #     StructField("hashes", StringType(),True),  # if after base64 in online and without dec in loaded: use StringType
    #     # StructField("hashes", BinaryType(),True),  # if before base64 in online and after base64 in loaded: use BinaryType
    #     StructField("__idconsec__", IntegerType(),True),
    # ])

    # # # Show v1
    # # saved_records = spark.createDataFrame(saved_records, schema=schema)
    # # records = spark.createDataFrame(records, schema=schema)
    # # print("Example (loaded one): ", saved_records.filter(saved_records.__idconsec__ == 1).collect())
    # # print("Example (online one): ", records.filter(records.__idconsec__ == 1).collect())

    # # Show v2
    # saved_records = saved_records.toDF()
    # records = records.toDF()
    # print("Example (loaded one): ", saved_records.filter(saved_records._3 == 1).collect())
    # print("Example (online one): ", records.filter(records._3 == 1).collect())

    # saved_records.show(1)
    # records.show(1)
    """FOR DEBUG on Loading"""

    return records


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
        log.warn(f"Using optimal parameters: {B}, {R}")
    else:
        B, R = args.b, args.r
        _B, _R = optimal_param(args.threshold, args.num_perm)
        log.warn(f"Using parameters: {B}, {R}, with optimal parameters: {_B}, {_R}")

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

    if args.table:
        df = spark.read.format("bigquery").option("table", args.table).load()
    elif args.data_path:
        files = os.listdir(args.data_path)
        records = load_and_hash(files, args)
