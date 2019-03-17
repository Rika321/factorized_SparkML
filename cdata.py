import numpy as np
import random
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("name")
parser.add_argument("num_feature_s")
parser.add_argument("num_feature_r")
parser.add_argument("num_item_s")
parser.add_argument("num_item_r")
args = parser.parse_args()


name          = args.name
num_feature_s = int(args.num_feature_s)
num_feature_r = int(args.num_feature_r)
num_item_s    = int(args.num_item_s)
num_item_r    = int(args.num_item_r)

print("making data "+ args.name +"....")
print("num_feature_s:"+args.num_feature_s)
print("num_feature_r:"+args.num_feature_r)
print("num_item_s:"+args.num_item_s)
print("num_item_r:"+args.num_item_r)

out_name = name + "_out.txt"
s_table_name = name+"_table_s.csv"
r_table_name = name+"_table_r.csv"

dl_path = "./test_data/"+name
if not os.path.exists(dl_path):
    print("making new dir "+ name +"....")
    os.makedirs(dl_path)

with open("./test_data/"+name+"/"+out_name, "w") as f:
    f.write("num_feature_s:"+str(num_feature_s)+"\n")
    f.write("num_feature_r:"+str(num_feature_r)+"\n")
    f.write("num_item_s:"+str(num_item_s)+"\n")
    f.write("num_item_r:"+str(num_item_r)+"\n")


#s header
s_header = [""]*(num_feature_s+3)
s_header[0] = "sid"
s_header[1] = "y"
s_header[2] = "fk"
for i in range(1,num_feature_s+1,1):
    s_header[2+i] = "fs_"+str(i)
s_header_str = ",".join(s_header)


def one_line_s(i):
    line = [0]*(num_feature_s+3)
    line[0] = str(i)
    line[1] = str(random.uniform(0, 1))
    line[2] = str(random.randint(0, num_item_r-1))
    for i in range(1,num_feature_s+1,1):
        line[2+i] = str(random.uniform(0, 1))
    return ",".join(line)


with open("./test_data/"+name+"/"+s_table_name, "w") as f:
    f.write(s_header_str+"\n")
    for i in range(num_item_s):
        row = one_line_s(i)
        if i<(num_item_s-1):
            row +="\n"
        f.write(row)

#r header
r_header = [""]*(num_feature_r+1)
r_header[0] = "rid"
for i in range(1,num_feature_r+1,1):
    r_header[0+i] = "fr_"+str(i)
r_header_str = ",".join(r_header)


def one_line_r(i):
    line = [0]*(num_feature_r+1)
    line[0] = str(i)
    for i in range(1,num_feature_r+1,1):
        line[0+i] = str(random.uniform(0, 1))
    return ",".join(line)

with open("./test_data/" +name+"/" + r_table_name, "w") as f:
    f.write(r_header_str+"\n")
    for i in range(num_item_r):
        row = one_line_r(i)
        if i<(num_item_r-1):
            row +="\n"
        f.write(row)
