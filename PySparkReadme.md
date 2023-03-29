# What's PySpark

Relation of PySpark to Spark: This is usually for local usage or as a client to connect to a cluster instead of setting up a cluster itself.

## Quick Start
+ Session: PySpark applications start with initializing `SparkSession` which is the entry point of PySpark as below.
```python
from pyspark.sql import SparkSession
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
```

+ Data: A PySpark DataFrame can be created via `pyspark.sql.SparkSession.createDataFrame` typically by passing a list of lists, tuples, dictionaries and pyspark.sql.Rows, a pandas DataFrame and an RDD consisting of such a list.
```python
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

# use row
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
# or use schema (need types)
df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
# or from RDD
rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])
df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])

# visualize
# All DataFrames above result same.
df.show()
print(df.columns)
df.printSchema()
df.show(1)
df.show(1, vertical=True)
# import and export, also support pandas, csv, parquet, orc..
df.toPandas()
df.write.csv('foo.csv', header=True)
spark.read.csv('foo.csv', header=True).show()
```

**Ops and Funcs.**  
=
PySpark supports various UDFs (*i.e. user-defined-functions*) and APIs to allow users to execute Python native functions. E.g.  
```python
import pandas
from pyspark.sql.functions import pandas_udf

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + 1

df.select(pandas_plus_one(df.a))
```

Details
=
+ Ops: `Collect`. `DataFrame.collect()` collects the distributed data to the driver side as the local data in Python. Note that this can throw an **out-of-memory error** when the dataset is too large to fit in the driver side because it collects all the data from executors to the driver side.  
In order to avoid throwing an out-of-memory exception, use `DataFrame.take()` or `DataFrame.tail()`. E.g. `df.take(1)` returns the operatable value of `df.show(1)`.

+ Ops: `Select`. For example, `DataFrame.select()` takes the Column instances that returns another **DataFrame**. We can also insert a column through: `WithColumn`
```python
df.select(df.c).show() # df.c is one of the column shown in creation
df.withColumn('upper_c', upper(df.c)) # insert a new column named upper_c with value of uppercased contents in df.c
```

+ Ops: `Groupby`. Data Grouping on the column named color and average values of else columns: `df.groupby('color').avg()`. Co-grouping around two groups and merged their values: `df1.groupby('id').cogroup(df2.groupby('id'))`

+ Ops: `Filter`. To select a subset of rows, use `DataFrame.filter()`. E.g. `df.filter(df.a == 1)`. *Maybe filter == collect with conditions?*

+ Ops: `RDD.FlatMap`. Return a new RDD by first **applying a function to all elements** of this RDD, and then flattening the results.

```python
>>> rdd = sc.parallelize([2, 3, 4])
>>> sorted(rdd.flatMap(lambda x: range(1, x)).collect())
[1, 1, 1, 2, 2, 3]
>>> sorted(rdd.flatMap(lambda x: [(x, x), (x, x)]).collect())
[(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
```

+ Ops: `RDD.Distinct`. Return a new RDD containing the **distinct elements** in this RDD.

+ Ops: `Drop`. Returns a new DataFrame that drops the specified column. E.g. `df.drop('age')`

+ Ops: `RDD.Join`. `Join` is similar to what it does in Dict: merging two RDDs with same keys.  
```python
>>> x = sc.parallelize([("a", 1), ("b", 4)])
>>> y = sc.parallelize([("a", 2), ("a", 3)])
>>> sorted(x.join(y).collect())
[('a', (1, 2)), ('a', (1, 3))]
```

## An illustration to [an example](https://github.com/weiyx16/text-dedup/blob/main/text_dedup/minhash_spark.py)

*This is a minhash function using spark.*  

+ Start up  
```python
# set up config. Equal to SparkSession.builder.appName(xx).config('key', 'value')
conf = SparkConf()
conf.set("spark.app.name", "MinHashLSH")
conf.set("spark.sql.debug.maxToStringFields", "100") #Default 25 (just for debugging); Maximum number of fields of sequence-like entries can be converted to strings in debug output. Any elements beyond the limit will be dropped and replaced by a "... N more fields" placeholder.
# conf.set('spark.executor.memory','50g') # they set it in pyspark start
# conf.set('spark.driver.memory','8g') # they set it in pyspark start; In client mode, this config must not be set through the SparkConf directly in your application, because the driver JVM has already started at that point. Instead, please **set this through the --driver-memory** command line option or in your default properties file.
# conf.set('spark.executor.cores','14') # they set it in pyspark start
# conf.set('spark.local.dir', '/tmp')
# conf.set('spark.driver.maxResultsSize','0') #Default 1GB; Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or **0 for unlimited**. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.

spark = SparkSession.builder.config(conf=conf).getOrCreate()
log: Logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # type: ignore
```

+ Read data
```python
# what's format of bigquery from google?
df = spark.read.format("bigquery").option("table", args.table).load()
# add new ids with monotonically_increasing_id(to ensure un-deduplicated)
df = df.withColumn("__id__", F.monotonically_increasing_id()).cache()
# Select two colums: __id__ and args.column(which we need to dedup) and Switch to rdd format. [RDD is for parallelization]
records = df.select("__id__", args.column).rdd
# Re-distributed rdd with args.num_perm * 2; Create a new RDD; and Persist this RDD with the default storage level (MEMORY_ONLY) using cache()
records = records.repartition(args.num_perm * 2).cache()
```

+ Generate hash
```python
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
    .cache()
)
```

## minHash in PySpark
+ TODO: [link](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.ml.feature.MinHashLSH.html)

## Questions
+ What's RDD. *i.e. Resilient Distributed Dataset* is the core of PySpark. RDD is compatible with external Hadoop. Ops: `Transformation` (lazy action; One RDD to another), `Action`(On the same RDD. intermediate op including previous transformations), `Persistent`(Hot cache in RAM).

+ What's Hadoop

+ Relation to Kubernetes

+ Driver v.s. Executor. driver就是我们编写的spark应用程序，用来创建sparkcontext或者sparksession，driver会和cluster mananer通信，并分配task到executor上执行。关于他们在client mode和cluster mode时候的不同功能：[Link](https://stackoverflow.com/questions/26562033/how-to-set-apache-spark-executor-memory).

+ Driver.Memory: Spark properties mainly can be divided into two kinds: one is related to deploy, like “spark.driver.memory”, “spark.executor.instances”, this kind of properties may not be affected when setting programmatically through SparkConf in runtime, or the behavior is depending on which cluster manager and deploy mode you choose, so it would be suggested to **set through configuration file or spark-submit command line options**; another is mainly related to Spark runtime control, like “spark.task.maxFailures”, this kind of properties can be set in either way.

+ **How to operate TB datas on machines with less RAM**: [ref](https://stackoverflow.com/questions/34824349/spark-execution-of-tb-file-in-memory).
Analysis about memory: (OpenWebText: 24M documents with 82GB raw text; For 1TB raw text: we assume 300M documents; for each documents, we have n hashes with each hash value as 32 or 64 bits; 300M\*8B\*n=2.4nGB; Assume we have 100 hashes; we can still occupy 240GB RAM; How could they make a 9000 hashes????)

[RDD Persistence](https://spark.apache.org/docs/3.3.2/rdd-programming-guide.html#rdd-persistence)
=
One of the most important capabilities in Spark is persisting (or caching) a dataset in memory across operations. When you persist an RDD, each node stores any partitions of it that it computes in memory and reuses them in other actions on that dataset (or datasets derived from it). This allows future actions to be much faster (often by more than 10x). Caching is a key tool for iterative algorithms and fast interactive use.

You can mark an RDD **to be persisted using the `persist()` or `cache()`** methods on it. The first time it is computed in an action, it will be kept in memory on the nodes. Spark’s cache is fault-tolerant – if any partition of an RDD is lost, it will automatically be recomputed using the transformations that originally created it.

In addition, each persisted RDD can be stored using a different storage *level*, allowing you, for example, to persist the dataset on disk, persist it in memory but as serialized Java objects (to save space), replicate it across nodes. These levels are set by passing a StorageLevel object (Scala, Java, Python) to `persist()`. The `cache()` method is a shorthand for using the default storage level, which is `StorageLevel.MEMORY_ONLY` (store deserialized objects in memory)

+ How set the partition numbers in rdd: https://blog.csdn.net/mzqadl/article/details/104217828  

+ What type of item should be in the dataframe: https://stackoverflow.com/a/32742294/11821194

## Characteristic
+ Lazy Compute
> PySpark DataFrames are lazily evaluated. They are implemented on top of RDDs. When Spark transforms data, it does not immediately compute the transformation but plans how to compute later. When actions such as collect() are explicitly called, the computation starts.
## Reference:
+ [Official PySpark](https://spark.apache.org/docs/3.1.1/api/python/getting_started/quickstart.html)