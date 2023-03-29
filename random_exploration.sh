set -x
pip install datasketch
pip install text-dedup
git clone https://github.com/weiyx16/text-dedup.git

##### ============ A toy Example ==============
python minhash.py \
  --path "/tmp/code/tiny_owt" \
  --cache_dir "./cache" \
  --output "/tmp/code/tiny_owt_dedup" \
  --column "text" \
  --local \
  --ngram 13 \
  --num_perm 10 \
  --threshold 0.7 \
  --batch_size 10000
  # --b 20 --r 450
  # for per: 10: best b=2, r=5

# An example using 2 openwebtext (2*460MB) [64Core 500G RAM]
# [03/23/23 08:38:55] INFO     Loading                         : 0.09s                                                                                                  minhash.py:357
#                     INFO     MinHashing                      : 11.12s                                                                                                 minhash.py:357
#                     INFO     Clustering                      : 1.57s                                                                                                  minhash.py:357
#                     INFO     Filtering                       : 1.49s                                                                                                  minhash.py:357
#                     INFO     Saving                          : 3.12s                                                                                                  minhash.py:357
#                     INFO     Total                           : 17.39s                                                                                                 minhash.py:357
#                     INFO     Before                          : 241804                                                                                                 minhash.py:359
#                     INFO     After                           : 241794   

# verify difference
ori_ds = load_dataset("text", data_dir="tiny_owt", sample_by="paragraph", split='train')
new_ds = load_from_disk('./tiny_owt_dedup/')
ori = set([l for l in ori_ds['text']])
new = set([l for l in new_ds['text']])

In [14]: for id, l in enumerate(list(ori)):
    ...:     if l not in new:
    ...:         print(id)
    ...:
729
33244
111319
170678
191534
# why it only includes 5?: set([l for l in ori_ds['text']]) = 241799 (unset: 241804)

########## ================ On OpenWebText ================
python minhash.py \
  --path "/tmp/code/openwebtext" \
  --cache_dir "./cache" \
  --output "/tmp/code/openwebtext_dedup" \
  --column "text" \
  --local \
  --ngram 13 \
  --num_perm 10 \
  --threshold 0.7 \
  --batch_size 10000

# An example using full openwebtext (64*460MB: 7.7M documents) [64Core 500G RAM] [10mins]
# [03/23/23 09:09:55] INFO     Loading                         : 215.65s                                                                                                minhash.py:357
#                     INFO     MinHashing                      : 267.05s                                                                                                minhash.py:357
#                     INFO     Clustering                      : 50.99s                                                                                                 minhash.py:357
#                     INFO     Filtering                       : 21.09s                                                                                                 minhash.py:357
#                     INFO     Saving                          : 87.38s                                                                                                 minhash.py:357
#                     INFO     Total                           : 642.16s                                                                                                minhash.py:357
#                     INFO     Before                          : 7736937                                                                                                minhash.py:359
#                     INFO     After                           : 7730251                                                                                                minhash.py:360
(notice loading + saving takes 50% time...)
(compared to toy examples, the time consumpation is linearly?)

Time from their repos: 42M documents: (~150GB?) 10hours + 80Core + 1.8TB RAM
With spark version: 40mins + 160Core + 640GB RAM

# Increase the hashes from 10 to 32 we have around 20% overhead (B=5, R=6)
# [03/23/23 09:20:14] INFO     Loading                         : 0.21s                                                                                                  minhash.py:357
#                     INFO     MinHashing                      : 287.61s                                                                                                minhash.py:357
#                     INFO     Clustering                      : 97.31s                                                                                                 minhash.py:357
#                     INFO     Filtering                       : 26.32s                                                                                                 minhash.py:357
#                     INFO     Saving                          : 94.78s                                                                                                 minhash.py:357
#                     INFO     Total                           : 506.24s                                                                                                minhash.py:357
#                     INFO     Before                          : 7736937                                                                                                minhash.py:359
#                     INFO     After                           : 7728681      

# Increase the hashes from 32 to 128 we have around 80% overhead (B=14, R=9) 
# [03/23/23 09:56:12] INFO     Loading                         : 0.21s                                                                                                                          minhash.py:357
#                     INFO     MinHashing                      : 470.30s                                                                                                                        minhash.py:357
#                     INFO     Clustering                      : 253.26s                                                                                                                        minhash.py:357
#                     INFO     Filtering                       : 33.99s                                                                                                                         minhash.py:357
#                     INFO     Saving                          : 98.80s                                                                                                                         minhash.py:357
#                     INFO     Total                           : 856.57s                                                                                                                        minhash.py:357
#                     INFO     Before                          : 7736937                                                                                                                        minhash.py:359
#                     INFO     After                           : 7730198                                                                                                                        minhash.py:360

# TODO: Test on OpenWebText 1+2 (if we want to use this, we need another function to convert back to our datasets)

######### About punctuation ... We normalize the spaces follow the gopher
In [5]: NON_ALPHA = re.compile("[^A-Za-z_0-9]")

In [6]: content = "a strange  sentence.. Just for test"

In [7]: NON_ALPHA.split(content)
Out[7]: ['a', 'strange', '', 'sentence', '', '', 'Just', 'for', 'test']

In [8]: list(filter(lambda x: len(x) > 0, NON_ALPHA.split(content)))
Out[8]: ['a', 'strange', 'sentence', 'Just', 'for', 'test']



########## Hash PySpark ################
# installation
# NOTICE: WARNING: Installation of scala and Spark is not necessary to run pyspark with python xxx
# But necessary to make it as a command line tool
cd /tmp/code
## Java
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
## Scala
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup -y
# verify scala -version
# locate at /home/aiscuser/.local/share/coursier/bin/scala
## Spark
wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar -xvf spark-3.3.2-bin-hadoop3.tgz
export SPARK_HOME=/tmp/code/spark-3.3.2-bin-hadoop3
export PATH=${SPARK_HOME}/bin/:$PATH
## PySpark
pip install pyspark[sql]

# gcloud dataproc jobs submit pyspark --cluster ${CLUSTER_NAME} \
#     --region us-central1 \
#     --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
#     --driver-log-levels root=WARN \
#     --properties="spark.executor.memory"="50g","spark.driver.memory"="8g","spark.executor.cores"="14" \
#     minhash_spark.py -- \
#     --table "huggingface-science-codeparrot.the_stack_java.java" \
#     --output "gs://chenghao-data/dataproc_output/deduplicated"

# Usage
## Interactive: Spark

## pyspark-submit
# Usage: spark-submit [options] <app jar | python file | R file> [app arguments]
# Usage: spark-submit --kill [submission ID] --master [spark://...]
# Usage: spark-submit --status [submission ID] --master [spark://...]
# Usage: spark-submit run-example [options] example-class [example args]

spark-submit --executor-memory=50G --driver-memory=8G --deploy-mode=client --executor-cores=64 minhash_spark.py \
  --data_path "/tmp/code/tiny_owt" \
  --output "/tmp/code/tiny_owt_dedup_sparkv2" \
  --column "text" \
  --ngram 13 \
  --num_perm 10 \
  --threshold 0.7

## python
-> Save as json. The dedup data size is similar to the previous one.
python minhash_spark.py \
  --data_path "/tmp/code/tiny_owt" \
  --output "/tmp/code/tiny_owt_dedup_spark" \
  --column "text" \
  --ngram 13 \
  --num_perm 10 \
  --threshold 0.7

貌似pyspark和python启动是一样的。

# Test with 2 openwebtext.1.txt
# After dedup: ~1/2 of original one;

# Test on larger datasets like openwebtext
> --ngram 13 --num_perm 10 --threshold 0.7; 13:14:27 begin data loading; 