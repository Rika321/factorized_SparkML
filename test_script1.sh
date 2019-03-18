#!/usr/bin/env bash
#############################################################################################
hdfs dfs -rm -r /test_data
hdfs dfs -mkdir /test_data
python cdata.py test01 50 400 1000000 10
hdfs dfs -mkdir /test_data/test01
hadoop fs -put ./test_data/test01/test01_table_s.csv /test_data/test01
hadoop fs -put ./test_data/test01/test01_table_r.csv /test_data/test01
# spark-submit factorized.py test01 5
# spark-submit mllib.py      test01 5 4
# spark-submit mllib.py      test01 5 6
# spark-submit mllib.py      test01 5 8
# spark-submit mllib.py      test01 5 10
# spark-submit mllib.py      test01 5 8

