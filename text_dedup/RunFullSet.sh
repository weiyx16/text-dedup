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
cd hashes
ls -d $PWD/* > ../data_paths.txt
cat data_paths.txt

# e.g.
# /tmp/code/OWT12 # we will locate /tmp/code/OWT12_hashes
# /tmp/code/PILECC-1 # we will locate /tmp/code/PILECC-1_hashes
# /tmp/code/PILECC-2 # we will locate /tmp/code/PILECC-2_hashes

spark-submit --executor-memory=50G --driver-memory=1200G --deploy-mode=client --executor-cores=64 minhash_spark_loadHashAndDedup.py   --data_path_file "data_paths.txt"   --output "/tmp/code/dedup_ids"   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8
## > w/o CC202104
# > total items found:  2559154195 // 5(bucket)
# > duplicate items found:  14168413 ~ 3%
# > time: 1.1h
# memory leak during saving

## > w/ CC202104 
# 372G hashes  [C4 have shorter documents.]
# 232G for C4 and 140G for others
 29G     ./hashes/C4-P8_hashes
 15G     ./hashes/CC202050_ab_hashes
 9.5G    ./hashes/C4-P1_ab_hashes
 29G     ./hashes/C4-P4_hashes
 15G     ./hashes/CC202050_ac_hashes
5.3G    ./hashes/PileCC_part0of2_ab_hashes                          
5.3G    ./hashes/PileCC_part0of2_aa_hashes                                     
21G     ./hashes/OWT12_hashes
4.7G    ./hashes/PileCC_part1of2_aa_hashes
9.5G    ./hashes/C4-P1_ac_hashes
18G     ./hashes/CC202104_ac_hashes
5.3G    ./hashes/PileCC_part0of2_ac_hashes
29G     ./hashes/C4-P5_hashes
9.5G    ./hashes/C4-P1_aa_hashes
15G     ./hashes/CC202050_aa_hashes
29G     ./hashes/C4-P2_hashes
18G     ./hashes/CC202104_ab_hashes
29G     ./hashes/C4-P6_hashes
29G     ./hashes/C4-P7_hashes
4.7G    ./hashes/PileCC_part1of2_ab_hashes
4.7G    ./hashes/PileCC_part1of2_ac_hashes
18G     ./hashes/CC202104_aa_hashes
29G     ./hashes/C4-P3_hashes
372G    ./hashes/

# > total items found:  2969626370 // 5(bucket)
# > # > duplicate items found:  18219965 ~ 3% (but we test on OWT12-Exact Dedup ~12%; test on original OWT1: Dedup 0.5%) [maybe for C4: not dedup; but for others; 3%/40% occupation ~ 7.5%]
# > time; load+edges is fast; slow in computing connected components
# memory leak during saving; [但是看文件又存下来了233；确认load的情况: reload with 18219965]

-------------=========== 
> Do it with two splits and we union them. We still face the RAM problems
## 1. do on CC202104 + CC202050 first with 99G hashes: 749851490//5: duplicate: 1934130; 1.3% 这里的重复率很低 (22mins)
## 2. do on C4 + PileCC + OWT with 273G hashes: 2219774880//5; duplicate: 10903134
## 5. do on PileCCOWT+CC202104: 812738825//5; duplicate: 5695028
## 6. do on PileCCOWT+CC202050: 741645965//5; duplicate: 5537122; ~3.5%; 主要是OWT自己内部的重复（~4M）

## [False] 3. do on C4+CC202104: 2227980405//5; duplicate: 7533394 [memory bug.. 一样的情况，memory leak了，但是存下来了] 
## [False] 4. do on C4+CC202050: 2156887545//5; duplicate: 6876497 [memory bug..]

# 4.1 do on C4-half1+CC202050: 1248130665//5; duplicate: 3519469
# 4.2 do on C4-half2+CC202050: 1248136195//5; duplicate: 3518651
# 3.1 do on C4-half1+CC202104: 1319223525//5; duplicate: 3921641
# 3.2 do on C4-half2+CC202104: 1319229055//5; duplicate: 3920134
# addition: C4 (we don't need it, have be included in C4 + PileCC + OWT)

Then we merge the repeated results;
Sum = 1934130 + 10903134 + 5695028 + 5537122 + 7533394 + 6876497 = 38479305
-------------=========== 
最终用的是global duplication: 18219965的版本。

######
+++++++++ --------- BEGIN REMOVAL --------- +++++++++
######
疑问：
1. 到底有没有跨数据集的去重；比如OWT12自己remove了K，OWT12和Others remove了K+n，现在n很小
虽然当时在OWT1-1+OWT1-2 和 OWT1-1+OWT1-3上测没问题
额外测试方式：
PileCC自己(P1+P2) Dedup（Hash处理的时候Pile分为1和2）: (P1+P2: 337408)；
PileCC-P1自己 Dedup: 168482；PileCC-P2自己 Dedup: 135724；合计304206
PileCC+Others Dedup: total: 588764 (P1: 302567; P2: 286197)；
看起来是能cross validation的

/output/azcopy copy "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/dedup_ids/dedup_ids_global/?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" /tmp/code --recursive

# OWT12 [done]
before / after: 32755440 / 28701515 = 4053925 (OWT12 self: 4047890, 这样一看数据集重复的好少啊。但是Pile那边相对多一点，多了100%的removal)

export DATASET="OWT12"
export DATA_FOLDER="/tmp/code/text-dedup-github/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyRemove.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids_global"  --column "text"   --ngram 13   --num_perm 64   --threshold 0.8

# PILECC-P1 [done]
P1aa: before / after: 8471944 / 8374749
P1ab: before / after: 8458944 / 8357748
P1ac: before / after: 8479927 / 8375751
Total: 25410815 / 25108248 => 302567 / 25410815 = 1.2%
export DATASET="P1ac"
export DATA_FOLDER="/tmp/code/text-dedup-github/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyRemove.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids_global"  --column "text"   --ngram 13   --num_perm 64   --threshold 0.8

/output/azcopy copy PileCC_part0of2/  "https://vlpretraineastus.blob.core.windows.net/crawl-text/cc_merged/fuzzy_minhashLsh_n13_h64_sim08/?sv=2021-04-10&st=2022-09-06T05%3A36%3A34Z&se=2025-09-05T05%3A36%3A00Z&sr=c&sp=racwdxltf&sig=8yIkemAX4aA8frrJoW1snsJB07suONjEHC5zR736MQw%3D" --recursive

# PILECC-P2 [done]
P2aa: before / after: 7430974 / 7337353
P2ab: before / after: 7432971 / 7337702
P2ac: before / after: 7423130 / 7325823
Total: 22287075 / 22000878 => 286197 / 22287075 = 1.3%
export DATASET="PILECC-P2ac"
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyRemove.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids_global"  --column "text"   --ngram 13   --num_perm 64   --threshold 0.8

# C4-P1 [done]
P1aa: before / after: 15146125 / 14948141
P1ab: before / after: 15147998 / 14937727
P1ac: before / after: 15143571 / 14925870
Total: 45437694 / 44811738 => 625956 / 45437694 = 1.4%
export DATASET="P1aa"
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyRemove.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids_global"   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8

# C4-P2-8
C4-P2: before / after: 45437930 / 44746433
C4-P3: before / after: 45437115 / 44702321
C4-P4: before / after: 45437531 / 44662681
C4-P5: before / after: 45437007 / 44624837
C4-P6: before / after: 45438137 / 44594927
C4-P7: before / after: 45437485 / 44561870
C4-P8: before / after: 45438747 / 44532168
Total: 317464536 / 313000000 => 4464536 / 317464536 = 1.4%
export DATASET="C4-P5"
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=800G --deploy-mode=client --executor-cores=64 minhash_spark_onlyRemove.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids_global"   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8

# CC2020-50
aa: before / after: 22583456 / 21508388
ab: before / after: 22657412 / 21565213
ac: before / after: 22634995 / 21537438
Total: 67875863 / 64611039 => 3264824 / 67875863 = 4.8%
export DATASET="CC202050_aa"
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyRemove.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids_global"   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8

# CC2021-04
aa: before / after: 27340796 / 26002567
ab: before / after: 27406106 / 26052101
ac: before / after: 27347533 / 25991986
Total: 82094435 / 78046654 => 4047771 / 82094435 = 4.9%
export DATASET="CC202104_ab"
export DATA_FOLDER="/tmp/code/text-dedup/text_dedup/$DATASET"
spark-submit --executor-memory=50G --driver-memory=400G --deploy-mode=client --executor-cores=64 minhash_spark_onlyRemove.py   --data_path $DATA_FOLDER --dedup_ids "/tmp/code/dedup_ids_global"   --column "text"   --ngram 13   --num_perm 64   --threshold 0.8