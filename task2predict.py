from pyspark import SparkContext
SparkContext.setSystemProperty("spark.driver.memory", "4g")
import json
import math
sc=SparkContext()
# import time
# start=time.time()

#
#
# input_test_path="test_review.json"
# input_path="task2_model.json"
# output_path="task2_output.json"

import sys
input_test_path=sys.argv[1]
input_path=sys.argv[2]
output_path=sys.argv[3]


# len_boolean_vector=15324 #This value need to be imported by model.jsoo

#
original=sc.textFile(input_path).map(lambda a:json.loads(a)).map(lambda element:(element["user"], element["business"], element["len_boolean_vector"])).collect()
# original_rdd=sc.textFile(input_path).filter(lambda a:a not in '[]').map(lambda line:[json.loads(line) for line in handle])
# with open(input_path, 'r') as handle:
#      json_data = [json.loads(line) for line in handle]
user_profile_code=original[0][0]
business_profile_coded=original[0][1]
len_boolean_vector=original[0][2]

def dictolist(input_dic):
    output=[]
    for i in user_profile_code:
        output.append((i,input_dic[i]))
    return output

#
# user_profile_coded_rdd=sc.parallelize(dictolist(user_profile_code))
# business_profile_coded_rdd=sc.parallelize(dictolist(business_profile_coded))

#input: a list of code
#output: the original boolean vector as profile
def converttooriginal(input_list):
    input=list(input_list)
    output=[]
    for i in range(len(input)-1):
        temp_binary_string=bin(input[i])[2:].zfill(32)
        temp_boolean=[True if x=='1' else False for x in temp_binary_string]
        output+=temp_boolean
    last_num_bit=len_boolean_vector-len(output)
    temp_binary_string=bin(input[-1])[2:].zfill(last_num_bit)
    temp_boolean = [True if x == '1' else False for x in temp_binary_string]
    output+=temp_boolean
    return output

def cosine_similarity(user, business):
    user_profile=converttooriginal(user_profile_code[user])
    business_profile=converttooriginal(business_profile_coded[business])
    a=user_profile and business_profile
    n=sum(a)
    d=math.sqrt(sum(user_profile))*(math.sqrt(sum(business_profile)))
    return float(n/d)


test_file_list=sc.textFile(input_test_path).map(lambda a:json.loads(a))\
    .filter(lambda element:element["user_id"] in user_profile_code and element["business_id"] in business_profile_coded)\
    # .collect()


similar_pair_sim=test_file_list.filter(lambda i:cosine_similarity(i['user_id'], i['business_id'])>=0.01)\
    .map(lambda i:{"user_id":i['user_id'], "business_id":i["business_id"], "sim":cosine_similarity(i['user_id'], i['business_id'])})\
    .collect()



with open(output_path, "w") as f:
    for i in similar_pair_sim:
        json.dump(i, f)
        f.write('\n')
# print("Duration:", time.time()-start)
# counter=0
# v=[]
# for i in test_file_list:
#     if cosine_similarity(i['user_id'], i['business_id'])>=0.01:
#         # v.append(cosine_similarity(i['user_id'], i['business_id']))
#         counter+=1
# print(float(counter/len(test_file_list)))
# print(time.time()-start)

# tttt=test_file_list.mapPartitions(lambda)













# user_profile_dic=user_profile_coded_rdd.map(lambda element:(element[0], converttooriginal(element[1]))).collectAsMap()
# business_profile_dic=business_profile_coded_rdd.map(lambda element:(element[0], converttooriginal(element[1]))).collectAsMap()
# user_profile_dic=user_profile_coded_rdd.collectAsMap()
# test_list=[]
# with open("test_review.json", "r") as f:
#     t=json.loads(f)
    # test_list.append(t)
    # while t:
    #     t=json.load(f)
    #     test_list.append(t)