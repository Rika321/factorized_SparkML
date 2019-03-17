from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,  SparkSession
from pyspark.ml.linalg import DenseVector,DenseMatrix
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors, VectorUDT, Matrix, DenseVector
from pyspark.sql.functions import col
from pyspark.sql import Column, Row
from pyspark.sql.functions import lit
from pyspark.sql import Column, Row
from pyspark.ml.regression import LinearRegression
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.ml.linalg import Vector as MLVector, Vectors as MLVectors
from pyspark.mllib.linalg import Vector as MLLibVector, Vectors as MLLibVectors
from pyspark.mllib.regression import  LabeledPoint
spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
sqlContext = SQLContext(spark)
import random
import time
import collections
import numpy as np
from collections import defaultdict
import argparse



def mllib_linear_regression(s_file, r_file, iter_ ):

    def data_process(s_file, r_file):
        table_s = spark.read.csv(s_file, inferSchema = True, header = True, sep = ",")
        table_r = spark.read.csv(r_file, inferSchema = True, header = True, sep = ",")

        table_r = table_r.withColumn("default", lit(1))
        table_s = table_s.select(*(col(c).cast("float").alias(c) for c in table_s.columns))
        table_r = table_r.select(*(col(c).cast("float").alias(c) for c in table_r.columns))
        table_s.registerTempTable("table_s")
        table_r.registerTempTable("table_r")


        table_joint = spark.sql("SELECT * FROM table_s LEFT JOIN table_r ON table_s.fk = table_r.rid")
        table_joint.registerTempTable("table_joint")
        table_joint = table_joint.select(*(col(c).cast("float").alias(c) for c in table_joint.columns))

        # make joint data
        col_size_s = len(table_s.columns)
        col_size_r = len(table_r.columns)
        feature_cols = table_joint.columns[3:col_size_s]+table_joint.columns[col_size_s+1:]

        vectorAssembler = VectorAssembler(inputCols = feature_cols, outputCol = 'X')
        table_assemble = vectorAssembler.transform(table_joint)
        exprs = [col(column).alias(column.replace(' ', '_')) for column in table_assemble.columns]
        #R(RID, X_R)
        Tdata = table_assemble.select(*exprs).selectExpr("y as y", "X as X")

        return Tdata.rdd
    #processing data
    #Sdata_rdd, Rdata_rdd, feat_size_s, feat_size_r,Sdata_size = data_pre_process(s_file, r_file)

    #processing data
    Tdata_rdd = data_process(s_file, r_file)
    trainingData = Tdata_rdd.map(lambda row: LabeledPoint(row.y, MLLibVectors.fromML(row.X)))

    lr_model = LinearRegressionWithSGD.train(trainingData, iterations=iter_, step=0.01, miniBatchFraction=1.0)
    W = list(lr_model.weights)
    return np.array(W)

parser = argparse.ArgumentParser()
parser.add_argument("name")
parser.add_argument("iter")
args = parser.parse_args()

name = args.name
s_table_name = name+"_table_s.csv"
r_table_name = name+"_table_r.csv"
re_name = name+"_MLlib_result.txt"

dl_path = "hdfs:/test_data/"+name+"/"
s_file = dl_path+s_table_name
r_file = dl_path+r_table_name

Learn_rate = 0.01
iter_ = int(args.iter)
display_step = 5

start_time = time.time()
W = mllib_linear_regression(s_file, r_file, iter_)
elapsed_time = time.time() - start_time
print("MLlib done:", elapsed_time)

with open("./test_data/"+name+"/"+re_name, "w") as f:
    f.write("MLlib ML done:"+str(elapsed_time)+str("\n"))