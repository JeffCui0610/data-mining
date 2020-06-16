import sys
imput_path=sys.argv[1]
test_input_path=sys.argv[2]
modelpath=sys.argv[3]
output_path=sys.argv[4]
cf_model=sys.argv[5]
if cf_model=="item_based":
    from pyspark import SparkContext

    SparkContext.setSystemProperty("spark.driver.memory", "4g")
    import json
    # import time
    #
    # start = time.time()
    import math

    sc = SparkContext()

    #
    # modelpath="modeltask3.json"
    # imput_path="train_review.json"
    # # business_avg_path="business_avg.json"
    # output_path="task3_case1_output.json"

    N = 9

    model = sc.textFile(modelpath).map(lambda element: json.loads(element)) \
        .map(lambda element: (
    (element["b1"], (element["b2"], element["sim"])), (element["b2"], (element["b1"], element["sim"])))) \
        .flatMap(lambda a: a).groupByKey().map(lambda element: (element[0], list(element[1]))).collectAsMap()

    businessid_userid_star = sc.textFile(imput_path).map(lambda element: json.loads(element)). \
        map(lambda element: (element["business_id"], element["user_id"], element["stars"]))
    businessid_userid_pair = businessid_userid_star.map(lambda element: (element[0], element[1]))

    userAndBusines_stars = businessid_userid_star.map(lambda element: ((element[1], element[0]), element[2])) \
        .groupByKey().map(lambda a: (a[0], list(a[1]))).map(
        lambda element: (element[0], float(sum(element[1]) / len(element[1])))) \
        .collectAsMap()

    businessid_rateduser = businessid_userid_pair.groupByKey().map(lambda element: (element[0], list(element[1])))
    businessid_rateduser_dic = businessid_rateduser.collectAsMap()


    def predict(input_business_user):
        if input_business_user[0] not in model:
            return False
        similar_business_star = sorted(model[input_business_user[0]], key=lambda element: element[1])
        user = input_business_user[1]
        numerator = 0
        denominator = 0
        counter = 0
        for i in reversed(similar_business_star):  # in decreasing order of similarity
            if user in businessid_rateduser_dic[i[0]]:
                counter += 1
                if counter > N:
                    break
                numerator += userAndBusines_stars[(user, i[0])] * i[1]
                denominator += abs(i[1])
        if counter == 0:
            return False
        return float(numerator / denominator)

    '''
    test_busi_user_star = sc.textFile(test_input_path).map(lambda element: json.loads(element)). \
        map(lambda element: ((element["business_id"], element["user_id"]),1))

    test_busi_user_predictstar = test_busi_user_star.map(
        lambda element: (element[0], (predict(element[0]), element[1])))\
        .filter(lambda element: element[1][0] != False) \
        .map(lambda element: {"user_id": element[0][1], "business_id": element[0][0], "stars": predict(element[0])}) \
        .collect()
   '''
    test_busi_user_star = sc.textFile(test_input_path).map(lambda element: json.loads(element)). \
        map(lambda element: (element["business_id"], element["user_id"]))

    test_busi_user_predictstar = test_busi_user_star \
        .map(lambda element: {"user_id": element[1], "business_id": element[0], "stars": predict(element)}) \
        .filter(lambda element:element["stars"]!=False)\
        .collect()
    

    with open(output_path, "w") as q:
        for i in test_busi_user_predictstar:
            json.dump(i, q)
            q.write('\n')
    # print("Duration:", time.time() - start)
else:
    from pyspark import SparkContext

    SparkContext.setSystemProperty("spark.driver.memory", "4g")
    import json

    sc = SparkContext()
    # import time
    #
    # start = time.time()
    # modelpath = "modeltask3_case2.json"

    N = 9
    special=3.823989

    model = sc.textFile(modelpath).map(lambda element: json.loads(element)) \
        .map(lambda element: ((element["u1"], (element["u2"], element["sim"])), (element["u2"], (element["u1"], element["sim"])))) \
        .flatMap(lambda a: a).groupByKey().map(lambda element: (element[0], list(element[1]))).collectAsMap()

    # imput_path = "train_review.json"
    # business_avg_path="business_avg.json"
    # user_avg_path="user_avg.json"
    # output_path = "modeltask3_case2.json"

    # business_avg=sc.textFile(business_avg_path).map(lambda element:json.loads(element)).collect()[0]
    # user_avg=sc.textFile(user_avg_path).map(lambda element:json.loads(element)).collect()[0]

    businessid_userid_star = sc.textFile(imput_path).map(lambda element: json.loads(element)). \
        map(lambda element: (element["business_id"], element["user_id"], element["stars"]))
    userAndBusiness_star = businessid_userid_star.map(lambda element: ((element[1], element[0]), element[2])) \
        .groupByKey().map(lambda a: (a[0], list(a[1]))).map(
        lambda element: (element[0], float(sum(element[1]) / len(element[1])))) \
        .collectAsMap()

    # user_ratedbusiness=businessid_userid_pair.groupByKey().map(lambda element:(element[0], list(element[1])))
    user_ratedbusiness = businessid_userid_star.map(lambda element: (element[1], element[0])) \
        .groupByKey().map(lambda element: (element[0], list(element[1])))
    user_ratedbusiness_dic = user_ratedbusiness.collectAsMap()


    # all_user=list(set(businessid_userid_star.map(lambda element:element[1]).collect()))
    def predict(input_business_user):
        if input_business_user[1] not in model:
            return False
        similar_user_star = sorted(model[input_business_user[1]], key=lambda element: element[1])
        business = input_business_user[0]
        numerator = 0
        denominator = 0
        counter = 0
        for i in reversed(similar_user_star):  # in decreasing order of similarity
            if business in user_ratedbusiness_dic[i[0]]:
                counter += 1
                if counter > N:
                    break
                numerator += userAndBusiness_star[(i[0], business)] * i[1]
                denominator += abs(i[1])
        if counter == 0:
            return False
        return float(numerator / denominator)

    '''
    test_busi_user_star = sc.textFile(test_input_path).map(lambda element: json.loads(element)). \
        map(lambda element: ((element["business_id"], element["user_id"]), 1))

    # test_busi_user_predictstar=test_busi_user_star.map(lambda element:(element[0], (predict(element[0]), element[1])))\
    #     .filter(lambda element:element[1][0]!=False).collectAsMap()

    test_busi_user_predictstar = test_busi_user_star.map(
        lambda element: (element[0], (predict(element[0]), element[1]))) \
        .filter(lambda element: element[1][0] != False) \
        .map(lambda element: {"user_id": element[0][1], "business_id": element[0][0], "stars": predict(element[0])}) \
        .collect()
    '''
    test_busi_user_star = sc.textFile(test_input_path).map(lambda element: json.loads(element)). \
        map(lambda element: (element["business_id"], element["user_id"]))
    test_busi_user_predictstar = test_busi_user_star\
        .map(lambda element: {"user_id": element[1], "business_id": element[0], "stars": predict(element)}) \
        .filter(lambda element:element["stars"]!=False)\
        .collect()
    with open(output_path, "w") as f:
        for i in test_busi_user_predictstar:
            json.dump(i, f)
            f.write('\n')
    # print("Duration:", time.time() - start)