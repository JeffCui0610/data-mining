
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
SparkContext.setSystemProperty("spark.driver.memory", "4g")
import json
import binascii
import math

host = "localhost"



import sys
port=int(sys.argv[1])
output_path=sys.argv[2]

hash_parameter = [[0, 13, 89], [1, 113, 121], [2, 17, 67], [3, 19, 79], [4, 113, 137], [5, 17, 23], [6, 79, 113], [7, 11, 67], [8, 19, 113], [9, 29, 137], [10, 67, 79], [11, 19, 111], [12, 111, 137], [13, 7, 137], [14, 11, 167], [15, 19, 29], [16, 79, 137], [17, 19, 23], [18, 113, 121], [19, 11, 17], [20, 79, 133], [21, 67, 113], [22, 13, 441], [23, 89, 121], [24, 29, 113], [25, 17, 111], [26, 67, 197], [27, 19, 111], [28, 23, 111], [29, 23, 113], [30, 13, 89], [31, 67, 221], [32, 17, 67], [33, 17,31], [34, 7, 111], [35, 89, 111], [36, 79, 113], [37, 13, 137], [38, 89, 121], [39, 67, 121], [40, 23, 89], [41, 19, 29], [42, 13, 67], [43, 23, 79], [44, 13, 137], [45, 23, 29], [46, 17, 79], [47, 17, 23], [48, 7, 23], [49, 79, 177], [50, 11, 79], [51, 29, 111], [52, 23, 67], [53, 121, 137], [54, 7, 79], [55, 89, 1111]]


# hash_parameter=[[13, 29, 37], [13, 29, 59], [13, 29, 61], [13, 29, 101], [13, 29, 201], [13, 29, 301], [13, 37, 59], [13, 37, 61],
#                 [13, 37, 101], [13, 37, 201], [13, 37, 301], [13, 59, 61], [13, 59, 101], [13, 59, 201], [13, 59, 301], [13, 61, 101],
#                 [13, 61, 201], [13, 61, 301], [13, 101, 201], [13, 101, 301], [13, 201, 301], [29, 37, 59], [29, 37, 61], [29, 37, 101],
#                 [29, 37, 201], [29, 37, 301], [29, 59, 61], [29, 59, 101], [29, 59, 201], [29, 59, 301], [29, 61, 101], [29, 61, 201]]
num_group = 7  # we only have 56 set of parameters for hash function
scale = len(hash_parameter) // num_group
estimate=float("-inf")
# num_

sc = SparkContext()
sc.setLogLevel(logLevel="OFF")
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream(host, port).window(30, 10)

hash_trailing_zero=[float("-inf")] * len(hash_parameter)

with open(output_path, "w") as file:
    file.write("Time,Ground Truth,Estimation")
    def calculate_training_zero(input_value_string):
        return len(input_value_string) - len(input_value_string.rstrip('0'))
def one_city(data_list):
    distinct_city ={}
    binary_hashvalue = list()
    for i in data_list:
        current_city = json.loads(i)["city"]
        if current_city in distinct_city:
            pass
        else:
            distinct_city[current_city]=0
            temp = []
            for t in hash_parameter:
                hash_value = (t[2] * (int(binascii.hexlify(current_city.encode('utf8')), 16)) + t[1]) % 4096
                temp.append(bin(hash_value)[2:])
            binary_hashvalue.append(temp)
    return distinct_city, binary_hashvalue
def max0(binary_hashvalue):
    temp2 = []
    for i in range(len(hash_parameter)):
        max_trailing_zero = float("-inf")
        for t in range(len(binary_hashvalue)):
            if calculate_training_zero(binary_hashvalue[t][i]) <= max_trailing_zero:
                pass
            else:
                max_trailing_zero = calculate_training_zero(binary_hashvalue[t][i])

        temp_est = math.pow(2, max_trailing_zero)
        temp2.append(temp_est)
    return temp2

def h(time, data_r):

    data_list = data_r.collect()

    distinct_city, binary_hashvalue=one_city(data_list)

    temp2=max0(binary_hashvalue)
    temp3 = []
    for i in range(7):
        temp_list = temp2[i * scale:(i + 1) * scale]
        average_in_group = sum(temp_list) / len(temp_list)
        temp3.append(average_in_group)
    estimate_result = list(sorted(temp3))[(len(temp3) // 2)]


    with open(output_path, "a", newline='') as csvfile:
        csvfile.write("\n" + str(time) + "," + str(len(distinct_city)) + "," + str(int(estimate_result)))


lines.foreachRDD(h)

ssc.start()
ssc.awaitTermination()

