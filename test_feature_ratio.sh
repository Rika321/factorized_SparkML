#!/usr/bin/env bash
#############################################################################################
<<<<<<< HEAD
hdfs dfs -rm -r /test_data
hdfs dfs -mkdir /test_data
python cdata.py test01 50 400 1000000 10
=======
python cdata.py test01 1 400 1000000 10
hdfs dfs -mkdir /test_data
>>>>>>> 134af8797dedd000c548e9faa8c482b43d45cad6
hdfs dfs -mkdir /test_data/test01
hadoop fs -put ./test_data/test01/test01_table_s.csv /test_data/test01
hadoop fs -put ./test_data/test01/test01_table_r.csv /test_data/test01
spark-submit factorized.py test01 5
spark-submit mllib.py      test01 5
#############################################################################################
python cdata.py test02 50 400 2000000 10
hdfs dfs -mkdir /test_data/test02
hadoop fs -put ./test_data/test02/test02_table_s.csv /test_data/test02
hadoop fs -put ./test_data/test02/test02_table_r.csv /test_data/test02
spark-submit factorized.py test02 5
spark-submit mllib.py      test02 5
#############################################################################################
python cdata.py test04 50 400 4000000 10
hdfs dfs -mkdir /test_data/test04
hadoop fs -put ./test_data/test04/test04_table_s.csv /test_data/test04
hadoop fs -put ./test_data/test04/test04_table_r.csv /test_data/test04
spark-submit factorized.py test04 5
spark-submit mllib.py      test04 5
#############################################################################################
python cdata.py test06 50 400 6000000 10
hdfs dfs -mkdir /test_data/test06
hadoop fs -put ./test_data/test06/test06_table_s.csv /test_data/test06
hadoop fs -put ./test_data/test06/test06_table_r.csv /test_data/test06
spark-submit factorized.py test06 5
spark-submit mllib.py      test06 5
#############################################################################################
python cdata.py test08 50 400 8000000 10
hdfs dfs -mkdir /test_data/test08
hadoop fs -put ./test_data/test08/test08_table_s.csv /test_data/test08
hadoop fs -put ./test_data/test08/test08_table_r.csv /test_data/test08
spark-submit factorized.py test08 5
spark-submit mllib.py      test08 5
#############################################################################################
python cdata.py test00 50 400 10000000 10
hdfs dfs -mkdir /test_data/test00
hadoop fs -put ./test_data/test00/test00_table_s.csv /test_data/test00
hadoop fs -put ./test_data/test00/test00_table_r.csv /test_data/test00
spark-submit factorized.py test00 5
spark-submit mllib.py      test00 5
