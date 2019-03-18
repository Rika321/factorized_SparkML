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
sc = SparkContext.getOrCreate(conf=SparkConf())

import random
import time
import collections
import numpy as np
from collections import defaultdict
import argparse

def factorized_linear_regression(s_file, r_file, Learn_rate , iter_, display_step = 5, rep_ratio = 8):
    def data_pre_process(s_file, r_file):
        table_s = spark.read.csv(s_file, inferSchema = True, header = True, sep = ",")
        table_r = spark.read.csv(r_file, inferSchema = True, header = True, sep = ",")

        table_r = table_r.withColumn("default", lit(1))
        table_s = table_s.select(*(col(c).cast("float").alias(c) for c in table_s.columns))
        table_r = table_r.select(*(col(c).cast("float").alias(c) for c in table_r.columns))
        table_s.registerTempTable("table_s")
        table_r.registerTempTable("table_r")

        # make joint data
        col_size_s = len(table_s.columns)
        col_size_r = len(table_r.columns)
        feat_size_s, feat_size_r = col_size_s-3, col_size_r-1

        # make S RDD
        #S(sid, y, fk, [X_S])
        vectorAssembler = VectorAssembler(inputCols = table_s.columns[3:], outputCol = 'X_S')
        table_assemble = vectorAssembler.transform(table_s)
        exprs = [col(column).alias(column.replace(' ', '_')) for column in table_assemble.columns]
        Sdata = table_assemble.select(*exprs).selectExpr("sid as sid", "y as y", "fk as fk", "X_S as X_S")

        # make R RDD
        #R(rid, [X_R])
        vectorAssembler = VectorAssembler(inputCols = table_r.columns[1:], outputCol = 'X_R')
        table_assemble = vectorAssembler.transform(table_r)
        exprs = [col(column).alias(column.replace(' ', '_')) for column in table_assemble.columns]
        #R(RID, X_R)
        Rdata = table_assemble.select(*exprs).selectExpr("rid as rid", "X_R as X_R")

        return Sdata.rdd, Rdata.rdd, feat_size_s, feat_size_r, Sdata.count()

    def read_table_R(row):
        wr_xr = float(np.array(row.X_R).dot(W_R_bc.value))
        return (int(row.rid), wr_xr)

    def read_table_S(iterator):
        partition_result = np.zeros((1 + len(W_S_bc.value) + num_fk_bc.value))
        #partition_result[0] = Li
        #partition_result[1:1+len(W_S_bc.value)] = G_Li_s
        #partition_result[2+len(W_S_bc.value):] = G_Li_s
        for row in iterator:
            w_x_y = float (np.array(row.X_S).dot(W_S_bc.value)) + HR_dict_bc.value[int(row.fk)] - float(row.y)
    #         w_x_y = w_x - float(row.y)
            partition_result[0] += w_x_y**2 #Li
            partition_result[1:1+len(W_S_bc.value)] += np.array(row.X_S)*w_x_y*2
            partition_result[1+len(W_S_bc.value)+int(row.fk)] += w_x_y
        return [partition_result]

    def read_table_R_2(row):
        ws_xs_y = (HS_bc.value[int(row.rid)])
        result = np.array(row.X_R)*float(2.0*ws_xs_y)
        #np.array(row.X_R)
        return result

    #processing data
    Sdata_rdd, Rdata_rdd, feat_size_s, feat_size_r,Sdata_size = data_pre_process(s_file, r_file)

    #repartition_ratio
    prev_p =Sdata_rdd.getNumPartitions()
    Sdata_rdd = Sdata_rdd.repartition(rep_ratio)


    #initialize parameters
    W_S     = np.zeros(feat_size_s)
    W_R     = np.zeros(feat_size_r)

    HR_dict = defaultdict(int)
    num_fk  = 0
    W_S_bc     = sc.broadcast(W_S)
    W_R_bc     = sc.broadcast(W_R)
    num_fk_bc  = sc.broadcast(num_fk)
    #iter_
    
    for i in range (iter_):
        Rdata_result = Rdata_rdd.map(read_table_R).collect()
        for ele in Rdata_result:
            HR_dict[ele[0]] = ele[1]
        HR_dict_bc = sc.broadcast(HR_dict)

        if i == 0:
            num_fk = len(Rdata_result)
            num_fk_bc  = sc.broadcast(num_fk)

        Sdata_result = Sdata_rdd.mapPartitions(read_table_S).reduce(lambda accum, n: np.add(accum,n))
        loss    = Sdata_result[0]
        G_W_S   = np.array(Sdata_result[1:1+len(W_S_bc.value)])/Sdata_size

        HS      = Sdata_result[1+len(W_S_bc.value):]
        HS_bc   = sc.broadcast(HS)

        Rdata_result2 = Rdata_rdd.map(read_table_R_2).reduce(lambda accum, n: np.add(accum,n))
        G_W_R   = np.array(Rdata_result2)/Sdata_size

        W_S = np.add((-1.0)*Learn_rate*G_W_S,  W_S)
        W_R = np.add((-1.0)*Learn_rate*G_W_R,  W_R)
        W_S_bc     = sc.broadcast(W_S)
        W_R_bc     = sc.broadcast(W_R)

        # if i%display_step == 0:
        #     print(str(i)+" step loss:", loss)
    return np.concatenate((W_S, W_R), axis=0)



parser = argparse.ArgumentParser()
parser.add_argument("name")
parser.add_argument("iter")
parser.add_argument("part_ratio")


args = parser.parse_args()

name = args.name
s_table_name = name+"_table_s.csv"
r_table_name = name+"_table_r.csv"
re_name = name+"_factorized_result.txt"

dl_path = "hdfs:/test_data/"+name+"/"
s_file = dl_path+s_table_name
r_file = dl_path+r_table_name

Learn_rate = 0.01
iter_ = int(args.iter)
part_ratio = int(args.part_ratio)
display_step = 5

start_time = time.time()
W = factorized_linear_regression(s_file, r_file, Learn_rate, iter_, display_step, part_ratio)
elapsed_time = time.time() - start_time
print("factorized ML done:", elapsed_time)

with open("./test_data/"+name+"/"+re_name, "w") as f:
    f.write("factorized ML done:"+str(elapsed_time)+"\n")