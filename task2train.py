import pyspark
from pyspark import SparkContext
SparkContext.setSystemProperty("spark.driver.memory", "4g")
import json
import time
# start=time.time()

import math
sc=SparkContext()#.setSystemProperty("spark.driver.memory", "2g")
#
input_path="train_review.json"
stopwordsPath="stopwords"
output_path="task2_model.json"

import sys

input_path=sys.argv[1]
output_path=sys.argv[2]
stopwordsPath=sys.argv[3]




original_rdd=sc.textFile(input_path).map(lambda element:json.loads(element))
bid_reviewtext=original_rdd.map(lambda element:(element["business_id"], str(element["text"]))).\
    reduceByKey(lambda x,y:x+y).map(lambda a:(a[0],str(a[1])))
# uid_reviewtext=original_rdd.map(lambda element:(element["user_id"], str(element["text"]))).\
#     reduceByKey(lambda x,y:x+y).map(lambda a:(a[0],str(a[1])))
# allwords=bid_reviewtext.map(lambda a:a[1]).flatMap(lambda a:a).count()



stopwords = []
with open(stopwordsPath) as f:
    line = f.readline()
    line = line.replace('\n', '')
    stopwords.append(line)
    while line:
        line = f.readline()
        line = line.replace('\n', '')
        stopwords.append(line)


punc = ['(', '[', ',', '.', "!", '?', ':', ';', ']', ')',
        '%','\n','0','1','2','3','4','5','6','7','8','9','-','$','@','#','{','}','*','"']
def Func(lines):
    for l in punc:
        lines = lines.replace(l, ' ')
    lines = lines.lower()
    lines = lines.split()
    lines=list(filter(lambda x: x not in stopwords, lines))
    return lines
def Func2(lines):#This is used for count total number of words
    for l in punc:
        lines = lines.replace(l, ' ')
    lines = lines.lower()
    lines = lines.split()
    return lines

allwords=bid_reviewtext.map(lambda element:(element[0], Func2(element[1]))).map(lambda a:a[1]).flatMap(lambda a:a).count()
bid_reviewtext_select2=bid_reviewtext.map(lambda element:(element[0], Func(element[1])))
num_business=bid_reviewtext_select2.count()



extreme_rare_words_list=bid_reviewtext_select2.map(lambda a:a[1]).flatMap(lambda a:a).map(lambda a:(a,1))\
    .reduceByKey(lambda x,y:x+y).filter(lambda a:a[1]<0.000001*allwords).map(lambda element:element[0]).collect()
def deleteextremerate(input_list, extreme_rare_words_list):
    r_set=set(extreme_rare_words_list)
    lines=list(filter(lambda x: x not in r_set, input_list))
    return lines
bid_reviewtext_select1=bid_reviewtext_select2.map(lambda element:(element[0], deleteextremerate(element[1],extreme_rare_words_list)))


#input: all filtered words as in a list
#output: the frequency (count/length of input list) of filter words as a dictionary
def frequencyofitem(input_list):
    dic={}
    for i in input_list:
        if i not in dic:
            dic[i]=1
        else:
            dic[i]+=1
    for i in dic:
        dic[i]=float(dic[i]/len(input_list))
    return dic

businessid_words_freq=bid_reviewtext_select1.map(lambda element:(element[0], frequencyofitem(element[1])))
def unique_words_onedocument_helper(input_rddlist):
    visited=set()
    output=[]
    input_list=list(input_rddlist)
    for i in input_list:
        if i not in visited:
            visited.add(i)
            output.append((i,1))
    return output
def unique_words(input_rddlist):
    input_list=input_rddlist
    output=[unique_words_onedocument_helper(i) for i in input_list]
    output1=[]
    for i in output:
        output1=output1+i
    return output1

idf=bid_reviewtext_select1.map(lambda element:unique_words(element)).flatMap(lambda a:a).reduceByKey(lambda x,y:x+y).\
    map(lambda element:(element[0], math.log(float(num_business/element[1])))).collectAsMap()





def turn_to_TFIDF(input_chunk, idf):
    input_list=list(input_chunk)
    input_list=list(map(list, input_list))
    for i in range(len(input_list)):
        for t in input_list[i][1]:
            input_list[i][1][t]=input_list[i][1][t]*idf[t]
            # i[1][t]=i[1][t]*idf(t)
    return input_list

tfidf=businessid_words_freq.mapPartitions(lambda element:turn_to_TFIDF(element,idf))
from collections import Counter
def selecttop200(input_chunk):
    input_list1=list(input_chunk)
    input_list=[]
    for i in range(len(input_list1)):
        input_list.append((input_list1[i][0],Counter(input_list1[i][1]).most_common(200)))
    return input_list

businessid_top_tfidf=businessid_words_freq.mapPartitions(lambda element: selecttop200(element))
def makebusinessprofile(input_chunk):
    input_list1=list(input_chunk)
    output=[]
    for i in input_list1:
        temp_output=[]
        for t in i[1]:
            temp_output.append(t[0])
        output.append((i[0],temp_output))
    return output

business_profile=businessid_top_tfidf.mapPartitions(lambda element: makebusinessprofile(element))
business_profile_dic=business_profile.collectAsMap()


all_selected_words=business_profile.map(lambda element:element[1]).flatMap(lambda a:a).map(lambda a:(a,1)).reduceByKey(lambda x,y:x+y).map(lambda a:a[0])
all_selected_words_list=all_selected_words.collect()
words_vectorindex={}
counter=0
for i in all_selected_words_list:
    if i not in words_vectorindex:
        words_vectorindex[i]=counter
        counter=counter+1
len_boolean_vector=counter







def to_boolean_vector(input_list):
    output=[False]*len_boolean_vector
    for i in input_list:
        idx=words_vectorindex[i]
        output[idx]=True
    return output


def boolList2BinString(lst):
    binarysring=''.join(['1' if x else '0' for x in lst])
    output=[]
    num_bit=32
    for i in range(int(len_boolean_vector/num_bit)):
        output.append(int(binarysring[i*num_bit:(i+1)*num_bit],2))
    return output






business_profile_vector=business_profile.map(lambda element:(element[0], to_boolean_vector(element[1])))\
    .map(lambda a:(a[0],boolList2BinString(a[1])))
business_profile_vector_dic=business_profile_vector.collectAsMap()

def make_user_profile(input_list):
    words1=[]
    for i in input_list:
        words1=words1+business_profile_dic[i]
    word=list(set(words1))
    output=[False]*len_boolean_vector
    for i in word:
        output[words_vectorindex[i]]=True
    return output



u_profile_vector1=original_rdd.map(lambda element:(element["user_id"], element["business_id"]))\
    .groupByKey().map(lambda element:(element[0],list(element[1])))#.map(lambda element:(element[0], make_user_profile(element[1])))
u_profile_vector=u_profile_vector1.map(lambda a:(a[0], make_user_profile(a[1]))).map(lambda a:(a[0],boolList2BinString(a[1])))
u_profile_vector_dic=u_profile_vector.collectAsMap()


# l=[business_profile_vector_dic,u_profile_vector_dic]
a={"user":u_profile_vector_dic, "business":business_profile_vector_dic, "len_boolean_vector":len_boolean_vector}
with open(output_path, 'w') as file:
    json.dump(a,file)
    # json.dump(u_profile_vector_dic,file)


# print(time.time()-start)

















# u_profile_vector=original_rdd.map(lambda element:(element["user_id"], element["business_id"]))\
#     .groupByKey().map(lambda element:(element[0], list(element[1]))).mapPartitions(lambda element: make_user_profile(element))
# u_profile_vector_dic=u_profile_vector.collectAsMap()
#     for i in l:
#         file.write(json.dump(i))
# def makeuserprofile(input_chunk):
#     input_list=list(input_chunk)
#     output=[]
#     for i in input_list:
#         temp_list=[]
#         for t in i[1]:
#             temp_list+=business_profile_dic[t]
#         output.append((i[0], list(set(temp_list))))
#     return output

#
# u_profile=original_rdd.map(lambda element:(element["user_id"], element["business_id"])).\
#     groupByKey().map(lambda element:(element[0], list(element[1]))).mapPartitions(lambda element: makeuserprofile(element)).collectAsMap()
#
#
#
# l=[business_profile_dic,u_profile]
# with open('model.json', 'w') as file:
#     json.dump(l,file,indent=0)
    # for i in l:
    #     file.write(json.dump(i))




# u_profile_vector_dic=u_profile_vector.collectAsMap()

# def make_user_profile(input_chunk):
#     input_list=list(input_chunk)
#     output=[]
#     for i in input_list:
#         t_output=[False]*len_boolean_vector
#         for t in i[1]:
#             t_output=t_output or business_profile_vector_dic[i]
#         output.append((i[0], t_output))
#     return output



# def make_user_profile(input_list1):
#     input_list=list(input_list1)
#     output=[False]*len_boolean_vector
#     for i in input_list:
#         t_dic=business_profile_vector_dic[i]
#         output = output or t_dic
#     return output
# def make_user_profile(input_list):