import sys
imput_path=sys.argv[1]
output_path=sys.argv[2]
cf_model=sys.argv[3]
# imput_path="train_review.json"
# output_path="model3_1.json"
# cf_model="item_based"
if cf_model=="item_based":
    from pyspark import SparkContext
    SparkContext.setSystemProperty("spark.driver.memory", "4g")
    import json
    import math
    # import time
    # start=time.time()
    sepecial_pearson_value=1

    sc=SparkContext()

    # imput_path="train_review.json"
    # business_avg_path="$ASNLIB/publicdata/business_avg.json"
    # user_avg_path="$ASNLIB/publicdata/user_avg.json"
    #
    # business_avg=sc.textFile(business_avg_path).map(lambda element:json.loads(element)).collect()[0]
    # user_avg=sc.textFile(user_avg_path).map(lambda element:json.loads(element)).collect()[0]

    businessid_userid_star=sc.textFile(imput_path).map(lambda element:json.loads(element)).\
        map(lambda element:(element["business_id"],element["user_id"], element["stars"]))
    businessid_userid_pair=businessid_userid_star.map(lambda element:(element[0], element[1]))

    business_avg=businessid_userid_star.map(lambda element:(element[0], element[2]))\
        .groupByKey().map(lambda element:(element[0], list(element[1])))\
        .map(lambda element:(element[0], float(sum(element[1])/len(element[1]))))\
        .collectAsMap()
    # user_avg=businessid_userid_star.map(lambda element:(element[1], element[2]))\
    #     .groupByKey().map(lambda element:(element[0], list(element[1])))\
    #     .map(lambda element:(element[0], float(sum(element[1])/len(element[1]))))\
    #     .collectAsMap()

    userAndBusines_stars=businessid_userid_star.map(lambda element:((element[1], element[0]),element[2]))\
        .groupByKey().map(lambda a:(a[0], list(a[1]))).map(lambda element:(element[0], float(sum(element[1])/len(element[1]))))\
        .collectAsMap()

    businessid_rateduser=businessid_userid_pair.groupByKey().map(lambda element:(element[0], list(element[1])))
    businessid_rateduser_dic=businessid_rateduser.collectAsMap()

    all_businessid=businessid_userid_pair.groupByKey().map(lambda element:element[0]).collect()
    def intersection(lst1, lst2):
        temp = set(lst2)
        lst3 = set([value for value in lst1 if value in temp])
        return list(lst3)
    from itertools import combinations
    businessid_pairs=list(combinations(all_businessid,2))
    def find_similar_business_pair(imput_pair):
        # imput_pair_list=list(imput_pair)
        if len(intersection(businessid_rateduser_dic[imput_pair[0]],businessid_rateduser_dic[imput_pair[1]]))>=3:
            return True
        else:
            return False
    # similar_business_pair=sc.parallelize(businessid_pairs).filter(lambda element:find_similar_business_pair(element))
    similar_business_pair=list(filter(lambda x:find_similar_business_pair(x), businessid_pairs))

    #Input:input is a pair of business
    #Output: The pearson similarity

    def pearsonsimilarity(imput_pair):
        intersected_user=intersection(businessid_rateduser_dic[imput_pair[0]], businessid_rateduser_dic[imput_pair[1]])
        denominator1=0
        denominator2=0
        numerator=0
        for i in intersected_user:
            numerator+=(userAndBusines_stars[(i,imput_pair[0])]-business_avg[imput_pair[0]])*\
                       (userAndBusines_stars[(i,imput_pair[1])]-business_avg[imput_pair[1]])
            denominator1+=(userAndBusines_stars[(i,imput_pair[0])]-business_avg[imput_pair[0]])**2
            denominator2+=(userAndBusines_stars[(i,imput_pair[1])]-business_avg[imput_pair[1]])**2
        if denominator2 ==0 or denominator1==0:
            return sepecial_pearson_value
        else:
            return float(numerator/((math.sqrt(denominator1))*(math.sqrt(denominator2))))


    similar_business_pair_and_pearson_simi=sc.parallelize(similar_business_pair).map(lambda element:(element, pearsonsimilarity(element)))\
        .filter(lambda element:element[1]>0)

    output_1=similar_business_pair_and_pearson_simi.map(lambda element:{"b1":element[0][0], "b2":element[0][1], "sim":element[1]})\
        .collect()

    with open(output_path, "w") as f:
        for i in output_1:
            json.dump(i, f)
            f.write('\n')

    # print(time.time()-start)
else:
    from pyspark import SparkContext

    SparkContext.setSystemProperty("spark.driver.memory", "4g")
    import json

    sc = SparkContext()
    import time

    # sc.stop()
    sepecial_pearson_value = -1

    start = time.time()
    businessid_userid_star = sc.textFile(imput_path).map(lambda element: json.loads(element)). \
        map(lambda element: (element["business_id"], element["user_id"], element["stars"]))

    original_business_list = businessid_userid_star.map(lambda element: (element[0], 1)).groupByKey().map(
        lambda a: a[0]).collect()
    num_business = len(original_business_list)

    # imput_path = "train_review.json"
    # business_avg_path = "business_avg.json"
    # user_avg_path = "user_avg.json"
    # output_path = "modeltask3_case2.json"
    # business_avg_path="$ASNLIB/publicdata/business_avg.json"
    user_avg_path="../resource/asnlib/publicdata/user_avg.json"
    #
    # business_avg = sc.textFile(business_avg_path).map(lambda element: json.loads(element)).collect()[0]
    user_avg = sc.textFile(user_avg_path).map(lambda element: json.loads(element)).collect()[0]

    businessid_userid_star = sc.textFile(imput_path).map(lambda element: json.loads(element)). \
        map(lambda element: (element["business_id"], element["user_id"], element["stars"]))

    #
    # business_avg=businessid_userid_star.map(lambda element:(element[0], element[2]))\
    #     .groupByKey().map(lambda element:(element[0], list(element[1])))\
    #     .map(lambda element:(element[0], float(sum(element[1])/len(element[1]))))\
    #     .collectAsMap()
    '''
    user_avg=businessid_userid_star.map(lambda element:(element[1], element[2]))\
        .groupByKey().map(lambda element:(element[0], list(element[1])))\
        .map(lambda element:(element[0], float(sum(element[1])/len(element[1]))))\
        .collectAsMap()
    '''

    businessAndUser_star = businessid_userid_star.map(lambda element: ((element[0], element[1]), element[2])) \
        .groupByKey().map(lambda a: (a[0], list(a[1]))).map(
        lambda element: (element[0], float(sum(element[1]) / len(element[1])))) \
        .collectAsMap()

    businessid_code_dict = {}
    counter3 = 0
    for i in original_business_list:
        if i not in businessid_code_dict:
            businessid_code_dict[i] = counter3
            counter3 += 1
    num_business = counter3


    def busi_id_to_code(input_pair):
        return (input_pair[1], businessid_code_dict[input_pair[0]])


    # user_codedbusinessid=businessid_userid_star.map(lambda element(element[0], element[1])).map(lambda element:busi_id_to_code(element))

    user_codedbusinessid = businessid_userid_star.map(lambda element: (element[1], businessid_code_dict[element[0]])) \
        .groupByKey().map(lambda element: (element[0], list(set(element[1]))))

    code_businessid_dict = dict((v, k) for k, v in businessid_code_dict.items())
    
    hashes = [[913, 901], [13, 23], [1, 101], [17, 91],
              [387, 53], [11, 37], [2, 63], [41, 67],
              [91, 29], [3, 79], [73, 803], [8, 119],
              [119, 91], [73, 63], [19, 101], [552, 11],
              [101, 17], [91, 67], [17, 67], [913, 63],
              [3, 73], [8, 73], [3, 91], [17, 101],
              [13, 11], [13, 119], [13, 11], [3, 23],
              [8, 553], [41, 101], [803, 41], [41, 11], [3, 11], [13, 17], [13, 553], [8, 91], [91, 8], [23, 14],
              [29, 91],
              [901, 913], [37, 11], [91, 3], [101, 17], [67, 41], [41, 17], [17, 41], [913, 3], [3, 913], [387, 1],
              [387, 3],
              [803, 387], [387, 803], [803, 101], [101, 803], [787, 773]
              ]
    '''
    hashes=[]
    prime_list=[2,3,7,13,17,23,37,43,47,53]#,47,53,57]#,67,73]#,83,87,97,103,107]#,113,137,131,151,161,181,191,201,203,211,231]
    from itertools import combinations 
    t1=combinations(prime_list,2)
    t2=list(map(lambda x:list(x),t1))
    hashes=hashes+t2
    t3=list(map(lambda x:list(reversed(x)), t2))
    hashes=hashes+t3
    '''
    r = 1
    bond = len(hashes)//r


    def min_hash_value(input_set):
        output = [float("inf")] * (r * bond)
        counter = 0
        for i in hashes:
            for idx in input_set:
                t = (i[0] * idx + i[1]) % (num_business)
                if t < output[counter]:
                    output[counter] = t
            counter += 1
        return output


    # userid_hashvalues=user_codedbusinessid.map(lambda element:(element[0], min_hash_value(element[1]))).collect()
    signature = user_codedbusinessid.map(lambda element: (element[0], min_hash_value(element[1])))


    # businessid_minhashvalue=businessid_ratedcodeduserID_pair.map(lambda element:(element[0], min_hash_value(element[1]))).collect()
    # t1=time.time()
    # candidates=[]
    #
    # from itertools import combinations
    # for i in range(bond):
    #     print(time.time() - t1)
    #     temp_dict={}
    #     for t in range(len(userid_hashvalues)):
    #         if userid_hashvalues[t][1][i] not in temp_dict:
    #             temp_dict[userid_hashvalues[t][1][i]]=[userid_hashvalues[t][0]]
    #         else:
    #             temp_dict[userid_hashvalues[t][1][i]].append(userid_hashvalues[t][0])
    #     for j in temp_dict:
    #         if len(temp_dict[j])>1:
    #             candidates=candidates+list(combinations(temp_dict[j],2))
    # candidate=list(set(candidates))
    # print(time.time()-t1)

    def transfer_one_column(input_list):
        output = []
        for i in range(bond):
            output.append(((i, tuple(input_list[1][i*r:(i+1)*r])), [input_list[0]]))
        return output


    def make_pair(input):
        output = []
        num_value = len(input[1])
        all_hashvalue = list(sorted(list(input[1])))
        for i in range(num_value):
            for j in range(i + 1, num_value):
                output.append(((all_hashvalue[i], all_hashvalue[j]), 1))
        return output


    candidates = signature.flatMap(transfer_one_column).reduceByKey(lambda x, y: x + y).filter(
        lambda x: len(x[1]) > 1).flatMap(make_pair) \
        .reduceByKey(lambda x, y: x).map(lambda element: element[0])


    def intersection(lst1, lst2):
        temp = set(lst2)
        lst3 = set([value for value in lst1 if value in temp])
        return list(lst3)


    def union(list1, list2):
        return set(list(list1) + list(list2))


    def jaccard(input_pair):
        input_list = list(input_pair)
        return float(len(intersection(input_list[0], input_list[1])) / len(union(input_list[0], input_list[1])))


    # #
    # #
    # #
    # # #
    # # #
    # # #
    u_bid_code = user_codedbusinessid.collectAsMap()


    #
    # candidates_rdd=sc.parallelize(set(list(cand)))
    # #
    def find_similar_business_pair(imput_pair):
        # imput_pair_list=list(imput_pair)
        if len(intersection(u_bid_code[imput_pair[0]], u_bid_code[imput_pair[1]])) >= 3:
            return True
        else:
            return False


    similar_pair = candidates.filter(lambda element: jaccard((u_bid_code[element[0]], u_bid_code[element[1]])) >= 0.01) \
        .filter(lambda element: find_similar_business_pair(element) != False)

    import math


    def pearsonsimilarity(imput_pair):
        intersected_busi = intersection(u_bid_code[imput_pair[0]], u_bid_code[imput_pair[1]])
        denominator1 = 0
        denominator2 = 0
        numerator = 0
        for i in intersected_busi:
            numerator += (businessAndUser_star[(code_businessid_dict[i], imput_pair[0])] - user_avg[imput_pair[0]]) * \
                         (businessAndUser_star[(code_businessid_dict[i], imput_pair[1])] - user_avg[imput_pair[1]])
            denominator1 += (businessAndUser_star[(code_businessid_dict[i], imput_pair[0])] - user_avg[
                imput_pair[0]]) ** 2
            denominator2 += (businessAndUser_star[(code_businessid_dict[i], imput_pair[1])] - user_avg[
                imput_pair[1]]) ** 2
        if denominator2 == 0 or denominator1 == 0:
            return sepecial_pearson_value
        else:
            return float(numerator / ((math.sqrt(denominator1)) * (math.sqrt(denominator2))))


    similar_user_pair_and_pearson_simi = similar_pair.map(lambda element: (element, pearsonsimilarity(element))) \
        .filter(lambda element: element[1] > 0)

    output_1 = similar_user_pair_and_pearson_simi.map(
        lambda element: {"u1": element[0][0], "u2": element[0][1], "sim": element[1]}) \
        .collect()
   # print(r, bond)
    with open(output_path, "w") as f:
        for i in output_1:
            json.dump(i, f)
            f.write('\n')
