## Setup
sudo /opt/conda/bin/pip install datasketch text-dedup memory_profiler
git clone https://github.com/weiyx16/text-dedup.git

cd /tmp/code
## Java
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk htop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
## Scala
sudo chmod -R 777 /home/t-yixuanwei/.profile
sudo chmod -R 777 /home/aiscuser/.profile
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup -y
scala -version

# locate at /home/aiscuser/.local/share/coursier/bin/scala
## Spark
wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar -xvf spark-3.3.2-bin-hadoop3.tgz
export SPARK_HOME=/tmp/code/spark-3.3.2-bin-hadoop3
export PATH=${SPARK_HOME}/bin/:$PATH
## PySpark
sudo /opt/conda/bin/pip install pyspark[sql]



######
+++++++++ --------- BEGIN HASHES --------- +++++++++
######
cd text-dedup/text_dedup
rm PREV_ID

+++++++++ --------- OWT V12 --------- +++++++++ [done]
# download location
export DATASET=OWT12
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/OpenWebText12.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=800G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori


------>>>>>>>> PREV_ID: 32755440
------>>>>>>>> TIME: 28mins to reach set row number; 32mins to begin writing tmpwithid (1mins~5-6GB); 48mins finish the writing; 52mins show tqdm output; 70mins to write hashes (1mins~1GB); 90mins done; [21G hashes]
------>>>>>>>> Highest memory consumption: 780G
------>>>>>>>> Direct Deduplication: 4047890


> RUN on another machine with PREV_ID set to 100,000,000 (OWT with 30GB: ~8M rows)
+++++++++ --------- PILE-CC PART1 --------- +++++++++
echo 100000000 > PREV_ID
# download location
export DATASET=PILECC-P1
export PARTNAME=PileCC_part0of2
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

> split into 3 splits first
export len=$(cat $DATASET/$PARTNAME.dedup.ExactLen200.txt | wc -l) # 608031528
echo $len
export len=$(($len/3+1))
split -l $len $DATASET/$PARTNAME.dedup.ExactLen200.txt $DATASET/$PARTNAME.dedup.ExactLen200.txt.split
rm $DATASET/$PARTNAME.dedup.ExactLen200.txt

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

------>>>>>>>> PREV_ID: 125410815


> RUN on another machine with PREV_ID set to 200,000,000 (OWT with 30GB: ~8M rows)
+++++++++ --------- PILE-CC PART2 --------- +++++++++
echo 200000000 > PREV_ID
# download location
export DATASET=PILECC-P2
export PARTNAME=PileCC_part1of2
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

> split into 3 splits first
> a 500GB RAM machine can only afford this..
> it will be better to write each part into a single bash file
export len=$(cat $DATASET/$PARTNAME.dedup.ExactLen200.txt | wc -l) # 533122262
echo $len
export len=$(($len/3+1))
split -l $len $DATASET/$PARTNAME.dedup.ExactLen200.txt $DATASET/$PARTNAME.dedup.ExactLen200.txt.split
rm $DATASET/$PARTNAME.dedup.ExactLen200.txt

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

------>>>>>>>> PREV_ID: 222287075


> RUN on another machine with PREV_ID set to 300,000,000 (OWT with 30GB: ~8M rows)
+++++++++ --------- C4 PART1 --------- +++++++++
echo 300000000 > PREV_ID
# download location
export DATASET=C4-P1
export PARTNAME=C4_part0of8
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

> split into 3 splits first
export len=$(cat $DATASET/$PARTNAME.dedup.ExactLen200.txt | wc -l) # 432538597
echo $len
export len=$(($len/3+1))
split -l $len $DATASET/$PARTNAME.dedup.ExactLen200.txt $DATASET/$PARTNAME.dedup.ExactLen200.txt.split
rm $DATASET/$PARTNAME.dedup.ExactLen200.txt

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

------>>>>>>>> PREV_ID: 345437694


> RUN on another machine with PREV_ID set to 400,000,000 (OWT with 30GB: ~8M rows)
+++++++++ --------- C4 PART23456 --------- +++++++++
echo 400000000 > PREV_ID
# download location
export DATASET=C4-P2
export PARTNAME=C4_part1of8
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

export DATASET=C4-P3
export PARTNAME=C4_part2of8
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

export DATASET=C4-P4
export PARTNAME=C4_part3of8
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

export DATASET=C4-P5
export PARTNAME=C4_part4of8
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

export DATASET=C4-P6
export PARTNAME=C4_part5of8
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

------->>>>>>> RUNNING


> RUN on another machine with PREV_ID set to 800,000,000 (OWT with 30GB: ~8M rows)
+++++++++ --------- C4 PART78 --------- +++++++++
echo 800000000 > PREV_ID

export DATASET=C4-P7
export PARTNAME=C4_part6of8
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

export DATASET=C4-P8
export PARTNAME=C4_part7of8
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

------>>>>>>> PREV_ID: 890876232



> RUN on another machine with PREV_ID set to 600,000,000 (OWT with 30GB: ~8M rows)
+++++++++ --------- CC202050 --------- +++++++++
echo 1000000000 > PREV_ID
# download location
export DATASET=CC202050
export PARTNAME=CC-2020-50_id_cleaned
mkdir $DATASET; /output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/$PARTNAME.dedup.ExactLen200.txt?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" ./$DATASET/ --cap-mbps 3000

> split into 3 splits first
> a 500GB RAM machine can only afford this..
export len=$(cat $DATASET/$PARTNAME.dedup.ExactLen200.txt | wc -l) # 533122262
echo $len
export len=$(($len/3+1))
split -l $len $DATASET/$PARTNAME.dedup.ExactLen200.txt $DATASET/$PARTNAME.dedup.ExactLen200.txt.split
rm $DATASET/$PARTNAME.dedup.ExactLen200.txt

## Run hashes
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyHash.py   --data_path $DATA_FOLDER   --output ${DATA_FOLDER}_hashes   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

------->>>>>>>> PREV_ID 1067875863



> RUN on another machine with PREV_ID set to 600,000,000 (OWT with 30GB: ~8M rows)
> sentence count 1618973895
+++++++++ --------- CC202104 --------- +++++++++
echo 1200000000 > PREV_ID
same as prev



######
+++++++++ --------- BEGIN DEDUPLICATION --------- +++++++++
######
> FIXME: We need to gather hashes from different machines.
> FIXME: And we need to distributed the deduplicated ids.

## Run Deduplicate [?mins]
rm -r /tmp/code/dedup_ids
rm data_paths.txt
echo $WHAT_YOU_WANT >> data_paths.txt
cat data_paths.txt

# e.g.
# /tmp/code/OWT12 # we will locate /tmp/code/OWT12_hashes
# /tmp/code/PILECC-1 # we will locate /tmp/code/PILECC-1_hashes
# /tmp/code/PILECC-2 # we will locate /tmp/code/PILECC-2_hashes

spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_loadHashAndDedup.py   --data_path_file "data_paths.txt"   --output "/tmp/code/dedup_ids"   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8
## > w/o CC202104
# > total items found:  2559154195 // 5(bucket)
# > duplicate items found:  x



######
+++++++++ --------- BEGIN REMOVAL --------- +++++++++
######

## Run Remove [?mins]
export DATASET="OWT12"
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=200G --deploy-mode=client --executor-cores=64 minhash_spark_onlyRemove.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids"  --column "text"   --ngram 13   --num_perm 64   --threshold 0.8 --rm_ori

! DO ON others