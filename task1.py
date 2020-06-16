import pyspark
# PYTHONUNBUFFERED=1;PYSPARK_PYTHON=/usr/bin/python3;PYSPARK_DRIVER_PYTHON=/usr/bin/python3
'''
If I want to solve the risk of "out of memory", maybe we can try more operations as rdd

'''
from pyspark import SparkContext
SparkContext.setSystemProperty("spark.driver.memory", "4g")
import json
sc=SparkContext()
#import time
# sc.stop()
import sys
#start=time.time()


#
# input_data_path="train_review.json"
# output_path="task1_output.json"
input_data_path=sys.argv[1]
output_path=sys.argv[2]



businessid_userid_pair=sc.textFile(input_data_path).map(lambda element:json.loads(element)).\
    map(lambda element:(element["business_id"],element["user_id"]))

original_user_list=businessid_userid_pair.map(lambda element:element[1]).collect()
username_code_dict={}
counter3=0
for i in original_user_list:
    if i not in username_code_dict:
        username_code_dict[i]=counter3
        counter3+=1
num_users=counter3
def username_to_code(input_pair):
    return (input_pair[0], username_code_dict[input_pair[1]])

businessid_ratedcodeduserID_pair=businessid_userid_pair.map(lambda element:username_to_code(element)).groupByKey().\
    map(lambda element:(element[0],set(element[1])))
r=1


hashes = [[913, 901], [14, 23], [1, 101], [17, 91],
          [387, 552], [11, 37], [2, 63], [41, 67],
          [91, 29], [3, 79], [73, 803], [8, 119],
          [119,91],[73,63],[14,101],[552,11],
          [101,17],[91,67],[17,67],[913,63],
          [3,73], [8,73],[3,91], [17,101],
          [14,11], [14,119],[14,11],[3,8],
          [8,553],[41,101],[803,41],[41,11],[3,11],[13,17],[13,553],[8,91],[91,8],[23,14],[29,91],
          [901,913],[37,11],[91,3],[101,17],[67,41],[41,17],[17,41],[913,3],[3,913],[387,1],[387,3],
          [803,387],[387,803],[803,101],[101,803],[787,773],[7,37],[37,7],[43,3],[3,43]
          ]
bond=len(hashes)
def min_hash_value(input_set):
    output = [float("inf")] * (r * bond)
    counter=0
    for i in hashes:
        for idx in input_set:
            t = (i[0] * idx + i[1]) % (num_users)
            if t < output[counter]:
                output[counter]=t
        counter+=1
    return output
businessid_minhashvalue=businessid_ratedcodeduserID_pair.map(lambda element:(element[0], min_hash_value(element[1])))#.collect()

# candidates=[]
# for i in range(bond):
#     temp_dict={}
#     for t in range(len(businessid_minhashvalue)):
#         if businessid_minhashvalue[t][1][i] not in temp_dict:
#             temp_dict[businessid_minhashvalue[t][1][i]]=[businessid_minhashvalue[t][0]]
#         else:
#             temp_dict[businessid_minhashvalue[t][1][i]].append(businessid_minhashvalue[t][0])
#     for j in temp_dict:
#         if len(temp_dict[j])>1:
#             for j2 in range(len(temp_dict[j])):
#                 for j3 in range(j2+1,len(temp_dict[j])):
#                     candidates.append((temp_dict[j][j2],temp_dict[j][j3]))


def transfer_one_column(input_list):
    output=[]
    for i in range(bond):
        output.append(((i,tuple(input_list[1][i:i+1])),[input_list[0]]))
    return output

def make_pair(input):
    output=[]
    num_value= len(input[1])
    all_hashvalue = list(sorted(list(input[1])))
    for i in range(num_value):
        for j in range(i + 1, num_value):
            output.append(((all_hashvalue[i], all_hashvalue[j]), 1))
    return output


candidates = businessid_minhashvalue.flatMap(transfer_one_column).reduceByKey(lambda x, y: x + y).filter(lambda x: len(x[1]) > 1).flatMap(make_pair) \
    .reduceByKey(lambda x, y: x).map(lambda element:element[0])




bid_u_code=businessid_ratedcodeduserID_pair.collect()
bid_ucode_dic={}
for i in bid_u_code:
    if i[0] not in bid_ucode_dic:
        bid_ucode_dic[i[0]]=i[1]

def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3
def union(list1,list2):
    return set(list(list1)+list(list2))

def jaccard(input_pair):
    input_list=list(input_pair)
    return float(len(intersection(input_list[0],input_list[1]))/len(union(input_list[0],input_list[1])))






# candidates_rdd=sc.parallelize(set(list(candidates)))
similar_pair=candidates.filter(lambda element:jaccard((bid_ucode_dic[element[0]],bid_ucode_dic[element[1]]))>=0.05)\
    .map(lambda element:{"b1":element[0], "b2":element[1], "sim":jaccard((bid_ucode_dic[element[0]],bid_ucode_dic[element[1]]))}).collect()
    # .map(lambda element:{"b1":element[0][0], "b2":element[0][1], "sim":element[1]}).collect()
# testrdd=candidates_rdd.map(lambda element:jaccard((bid_ucode_dic[element[0]],bid_ucode_dic[element[1]])))
# output_1=similar_business_pair_and_pearson_simi.map(lambda element:{"b1":element[0][0], "b2":element[0][1], "sim":element[1]})\
#     .collect()


with open(output_path, "w") as f:
    for i in similar_pair:
        json.dump(i, f)
        f.write('\n')
#print("Duration:", time.time()-start)





