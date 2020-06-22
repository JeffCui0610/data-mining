#   PYTHONUNBUFFERED=1;PYSPARK_PYTHON=/usr/bin/python3;PYSPARK_DRIVER_PYTHON=/usr/bin/python3
# PYSPARK_PYTHON=/usr/bin/python3;PYSPARK_DRIVER_PYTHON=/usr/bin/python3;PYSPARK_SUBMIT_ARGS=--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11


import os
import pyspark
from pyspark import SparkContext
#pip install graphframes
import graphframes
import pyspark.sql.functions as F


os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

scConf = pyspark.SparkConf() \
    .setAppName('hw4') \
    .setMaster('local[3]')
sc = SparkContext(conf = scConf)
sc.setLogLevel("WARN")

import sys

# community_output_file_path="1.txt"
# input_file_path="ub_sample_data.csv"
# threshold=7
threshold=int(sys.argv[1])
input_file_path=sys.argv[2]
community_output_file_path=sys.argv[3]






from pyspark.sql import SQLContext


sqlContext = SQLContext(sc)




inputRawData = sc.textFile(input_file_path).map(lambda line: line.split(","))

user_businesslist = inputRawData.groupByKey().map(lambda x: (x[0], list(x[1]))).\
    filter(lambda element: element[0] != 'user_id')

user_businesslist_dict=user_businesslist.collectAsMap()


t0= user_businesslist.map(lambda s:s[0]).collect()
# t1=list(map(lambda a:[a[0:-1]], t0))
# t1=list(map(lambda x:tuple(map(str, x.split(', '))) , t0))
# vertices=sqlContext.createDataFrame(t1, ["id"])

def intersection(lst1, lst2):
    temp = set(lst2)
    lst3 = [value for value in lst1 if value in temp]
    return lst3

def determine_edge(input_pair):
    businesslist1=user_businesslist_dict[input_pair[0]]
    businesslist2=user_businesslist_dict[input_pair[1]]
    if len(intersection(businesslist1, businesslist2))>=threshold:
        return True
    else:
        return False

def select_pair(input):
    input_list=list(set(list(input)))
    output=[]
    for i in range(len(input_list)-1):
        for j in range(i+1, len(input_list)):
            # output.append((input_list[i],input_list[j]))
            if determine_edge((input_list[i],input_list[j]))==True:
                output.append((input_list[i],input_list[j]))
    return output

edge=select_pair(t0)

v=set()
for i in edge:
    for t in i:
        if t not in v:
            v.add(t)
vertices=sqlContext.createDataFrame(list(map(lambda a:[a], v)), ["id"])

edge+=list(map(lambda x:tuple(reversed(x)), edge))
edges=sqlContext.createDataFrame(edge, ["src", "dst"])
g=graphframes.GraphFrame(vertices, edges)



result = g.labelPropagation(maxIter=5).groupBy("label").agg(F.collect_list("id")).collect()
# result.select("id", "label").show()

output=[]
for i in result:
    output.append(list(sorted(i["collect_list(id)"])))
   

output.sort(key=lambda x:(len(x),x[0]))
file2=open(community_output_file_path, "w")
# for i in range(len(output)-1):
#     file2.write(''.join(str(s) for s in output[i]) + '\n')
# file2.write(''.join(str(s) for s in output[-1]))
# file2.close()
for i in output:
    for t in range(len(i)-1):
        file2.write("\'")
        file2.write(i[t])
        file2.write("\'")
        file2.write(",")
        file2.write(" ")
    file2.write("\'")
    file2.write(i[-1])
    file2.write("\'")
    file2.write('\n')
file2.close()
