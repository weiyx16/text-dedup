## Setup
sudo /opt/conda/bin/pip install datasketch
sudo /opt/conda/bin/pip install text-dedup
git clone https://github.com/weiyx16/text-dedup.git

cd /tmp/code
## Java
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
## Scala
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup -y
scala -version
# verify scala -version
# locate at /home/aiscuser/.local/share/coursier/bin/scala
## Spark
wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar -xvf spark-3.3.2-bin-hadoop3.tgz
export SPARK_HOME=/tmp/code/spark-3.3.2-bin-hadoop3
export PATH=${SPARK_HOME}/bin/:$PATH
## PySpark
sudo /opt/conda/bin/pip install pyspark[sql]


----- OWT V1 -----
## Run hashes [10 perm 30G->1G; 16mins]
rm PREV_ID
export DATA_FOLDER="/tmp/code/openwebtext"
spark-submit --executor-memory=50G --driver-memory=100G --deploy-mode=client --executor-cores=64 hashonly.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 10   --threshold 0.7

## Run Deduplicate [0.5mins]
rm data_paths.txt
echo $DATA_FOLDER >> data_paths.txt
cat data_paths.txt
spark-submit --executor-memory=50G --driver-memory=5G --deploy-mode=client --executor-cores=64 loadhash.py   --data_path_file "data_paths.txt"   --output "/tmp/code/dedup_ids"   --column "text"   --ngram 13   --num_perm 10   --threshold 0.7
# > total items found:  15473874 // 2(bucket)
# > duplicate items found:  7419

## Run Remove
spark-submit --executor-memory=50G --driver-memory=100G --deploy-mode=client --executor-cores=64 rmdup.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids"  --column "text"   --ngram 13   --num_perm 10   --threshold 0.7



----- OWT V2 -----
## Run hashes [64 perm 30G->4.8G; 25mins]
rm PREV_ID
export DATA_FOLDER="/tmp/code/openwebtext"
spark-submit --executor-memory=50G --driver-memory=100G --deploy-mode=client --executor-cores=64 hashonly.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8

## Run Deduplicate [0.5mins]
rm data_paths.txt
echo $DATA_FOLDER >> data_paths.txt
cat data_paths.txt
spark-submit --executor-memory=50G --driver-memory=10G --deploy-mode=client --executor-cores=64 loadhash.py   --data_path_file "data_paths.txt"   --output "/tmp/code/dedup_ids"   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8
# > total items found:  38684685 // 5(bucket)
# > duplicate items found:  4225

## Run Remove [1mins]
spark-submit --executor-memory=50G --driver-memory=100G --deploy-mode=client --executor-cores=64 rmdup.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids"  --column "text"   --ngram 13   --num_perm 64   --threshold 0.8



----- OWT V3 -----
## Run hashes [256 perm 30G->17G; 25mins+17 mins write hashes (1mins 1G)]
rm PREV_ID
export DATA_FOLDER="/tmp/code/openwebtext"
spark-submit --executor-memory=50G --driver-memory=100G --deploy-mode=client --executor-cores=64 hashonly.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 256   --threshold 0.8

## Run Deduplicate [4mins] [memory已经有点超了; add to 30G mem->3mins]
rm data_paths.txt
echo $DATA_FOLDER >> data_paths.txt
cat data_paths.txt
spark-submit --executor-memory=50G --driver-memory=20G --deploy-mode=client --executor-cores=64 loadhash.py   --data_path_file "data_paths.txt"   --output "/tmp/code/dedup_ids"   --column "text"   --ngram 13   --num_perm 256   --threshold 0.8
# > total items found:  131527929 // ?(bucket)
# > duplicate items found:  4471

## Run Remove [?mins]
spark-submit --executor-memory=50G --driver-memory=100G --deploy-mode=client --executor-cores=64 rmdup.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids"  --column "text"   --ngram 13   --num_perm 256   --threshold 0.8



----- OWT12 ------
## Run hashes [64 perm 82G->?G; ?mins]
rm PREV_ID
export DATA_FOLDER="/tmp/code/openwebtext12"
spark-submit --executor-memory=50G --driver-memory=200G --deploy-mode=client --executor-cores=64 hashonly.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8

## Run Deduplicate [?mins]
rm data_paths.txt
echo $DATA_FOLDER >> data_paths.txt
cat data_paths.txt
spark-submit --executor-memory=50G --driver-memory=20G --deploy-mode=client --executor-cores=64 loadhash.py   --data_path_file "data_paths.txt"   --output "/tmp/code/dedup_ids"   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8
# > total items found:  x // 5(bucket)
# > duplicate items found:  x

## Run Remove [?mins]
spark-submit --executor-memory=50G --driver-memory=200G --deploy-mode=client --executor-cores=64 rmdup.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids"  --column "text"   --ngram 13   --num_perm 64   --threshold 0.8