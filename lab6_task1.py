
from pyspark import SparkContext
SparkContext.setSystemProperty("spark.driver.memory", "4g")
sc=SparkContext()
import json
import binascii
import csv
import sys

# input_path_first="business_first.json"
# input_path_second="business_second.json"
# output_path="task1_output.csv"


input_path_first=sys.argv[1]
input_path_second=sys.argv[2]
output_path=sys.argv[3]


array_bit_len=10000
hash_parameter=[113,11,203]


array_bit=[0]*array_bit_len

first_city_rdd=sc.textFile(input_path_first).map(lambda element:json.loads(element)).map(lambda element:(element['city'],1))\
    .groupByKey().map(lambda element:element[0]).filter(lambda element:element!='')
first_city_code_dic=first_city_rdd.map(lambda element:(element, int(binascii.hexlify(element.encode('utf8')),16))).collectAsMap()


def calculate_hash_value(input_value):
    # return ((hash_parameter[0]*input_value+hash_parameter[1])%hash_parameter[2])%array_bit_len
    return (hash_parameter[0] * input_value + hash_parameter[1]) % array_bit_len


for i in first_city_code_dic:
    a=calculate_hash_value(first_city_code_dic[i])
    array_bit[a]=1


second_city_list=sc.textFile(input_path_second).map(lambda element:json.loads(element)).map(lambda element:element['city'])\
    .collect()

def check(input_city_string):
    if not input_city_string:
        return 0
    t=int(binascii.hexlify(input_city_string.encode('utf8')),16)
    if array_bit[calculate_hash_value(t)]==1:
        return 1
    else:
        return 0

result=[check(i) for i in second_city_list]

with open(output_path, "w") as file:
    writer = csv.writer(file,delimiter=' ')
    # for i in result:
    #     writer.writerow(i)
    writer.writerow(result)
