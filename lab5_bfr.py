import os
import random

import json
import csv
import sys

input_path=sys.argv[1]
num_clusters=int(sys.argv[2])
result_output_file=sys.argv[3]
intermediate_file_path=sys.argv[4]

def read_txt_file(filename):
    data=[]
    s = filename.readline()
    t = s.split(",")
    data.append(list(map(lambda e: float(e), t)))
    while s:
        s = filename.readline()
        t=s.split(",")
        if t==['']:
            break
        data.append(list(map(lambda e:float(e), t)))
    return data

def data_dic(data):
    output={}
    for i in data:
        output[i[0]]=i[1:]
    return output, len(data[0])-1

#input: the index of data point by default, but data point1
#count actually be the feature value of data point
def euclidean_distance(datapoint_1, datapoint_2):
    s=0
    if type(datapoint_1)==list:
        datapoint1=datapoint_1.copy()
    else:
        datapoint1=dic_data[datapoint_1]
    if type(datapoint_2)==list:
        datapoint2=datapoint_2.copy()
    else:
        datapoint2=dic_data[datapoint_2]
    for i in range(len(datapoint1)):
        s+=(datapoint1[i]-datapoint2[i])**2
    return s**(1/2)

def combination(list1):
    output=[]
    for i in range(len(list1)-1):
        for j in range(i+1, len(list1)):
            output.append((list1[i], list1[j]))
    return output


#input: list of data point index
def n_sum_sumsq(input_data_points_index):
    n=len(input_data_points_index)
    input_data_points=[dic_data[i] for i in input_data_points_index]
    Sum=[]
    for feature_idx in range(len(input_data_points[0])):
        t_list=[i[feature_idx] for i in input_data_points]
        Sum.append(sum(t_list)/len(t_list))
    sumsq=[]
    for feature_idx in range(len(input_data_points[0])):
        t_list=[(i[feature_idx])**2 for i in input_data_points]
        sumsq.append(sum(t_list)/len(t_list))
    return [n, Sum, sumsq]

def centroid(input_data_index_list):
    output=[]
    if type(input_data_index_list[0])!=float:
        input_datapoint=input_data_index_list.copy()
    else:
        input_datapoint=[dic_data[i] for i in input_data_index_list]
    for i in range(len(input_datapoint[0])):
        t_list=[t[i] for t in input_datapoint]
        output.append(sum(t_list)/len(t_list))
    return output

#hierarchical cluster only works on small sample batch of data points for initial centroid
# def hierarchical_cluster(input_data_points_index,num_cluster):


# The output is data point feature value list, NOT index
def kmeans_initial_centroid(input_data_index, num_centroid):
    if num_centroid==10:
        input_data_index=input_data_index.copy()
        output=[input_data_index[random.randint(0,len(input_data_index))]]
        '''For the first centroid'''
        # temp_dict={}
        # for i in input_data_index:
        #     temp_dict[i]=sum(map(lambda e:e**2, dic_data[i]))
        # output.append(max(temp_dict, key=temp_dict.get))
        # input_data_index.remove(output[-1])
        '''For the second centroid'''
        # temp_dict={}
        # for i in input_data_index:
        #     temp_dict[i]=euclidean_distance(i, output[0])
        # output.append(max(temp_dict, key=temp_dict.get))
        # input_data_index.remove(output[-1])
        # random.shuffle(input_data_index)
        # output+=input_data_index[:num_centroid-len(output)]
        # output2=[dic_data[i] for i in output]
        def help1(input_data, current_output):
            #calculate the distance of one data point to current output average point
            #min distance from one data point to points in current_output
            # centroid_distance=0
            min_distance=float("inf")
            for i in current_output:
                e=euclidean_distance(i, input_data)
                if e<min_distance:
                    min_distance=e
            # print(current_output)
            # centroid_distance=euclidean_distance(input_data, centroid(current_output))
            # return sum([centroid_distance, 6*min_distance])
            return min_distance
        for t in range(num_centroid-len(output)):
            temp_dict = {}
            for i in input_data_index:
                temp_dict[i]=help1(i, output)
            output.append(max(temp_dict, key=temp_dict.get))
            input_data_index.remove(output[-1])
        return output # a list of index of initial centroid
    else:
        random.shuffle(input_data_index)
        return input_data_index[:num_centroid]
#output: {cluster_index(1,2,,,):[all data point index in this cluster]}
def kmeans_assign_cluster(centroid_data_point, datapoint_index, num_cluster):
    cluster_data_dic={}
    for i in range(num_cluster):
        cluster_data_dic[i]=[]
    for i in datapoint_index:
        if dic_data[i] in centroid_data_point:
            c=centroid_data_point.index(dic_data[i])
            # if num_cluster==50:
            #     print(111)
        else:
            d=[euclidean_distance(t,i) for t in centroid_data_point]
            c=d.index(min(d))

        cluster_data_dic[c].append(i)

    return cluster_data_dic

def kmeans_update_centroid(kmeans_cluster, num_cluster,input_data_index):
    new_centroid=[0]*len(kmeans_cluster)
    p = []
    for i in kmeans_cluster:
        temp_centroid=centroid(kmeans_cluster[i])
        mind=float("inf")
        c=0
        # p=[]
        for t in input_data_index:
            if euclidean_distance(t, temp_centroid)<mind:
                if t not in p:
                    mind=euclidean_distance(t, temp_centroid)
                    c=t
        new_centroid[i]=dic_data[c]
        p.append(c)
    counter=0
    t=[]
    # def Diff(li1, li2):
    #     li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    #     return li_dif
    for i in input_data_index:
        if dic_data[i] in new_centroid:
            counter+=1

    # print(counter,  len(new_centroid))
        # new_centroid.append(centroid(kmeans_cluster[i]))
    return new_centroid

#Motivation: if new and old centroid is the same, return True
#               and the k_means need to stop
def kmeans_centroid_check(centroid, kmeans_cluster):
    new_centroid=kmeans_update_centroid(kmeans_cluster)
    for i in range(len(centroid)):
        if set(new_centroid[i]) != set(centroid[i]):
            return False, new_centroid
    return True, centroid

def kmeans_check_cluster(kmeans_cluster_old, centroid,input_data_index, num_cluster):
    kmeans_cluster_new=kmeans_assign_cluster(centroid, input_data_index, num_cluster)
    for i in kmeans_cluster_new:
        if set(kmeans_cluster_new[i])!=set(kmeans_cluster_old[i]):
            return False, kmeans_cluster_new
    return True, kmeans_cluster_new



num_iteration=7

def kmeans(input_data_index, num_cluster):
    centroid=kmeans_initial_centroid(input_data_index, num_cluster)
    kmeans_cluster=kmeans_assign_cluster(centroid,input_data_index,num_cluster)

    for i in range(num_iteration):


        centroid=kmeans_update_centroid(kmeans_cluster, num_cluster,input_data_index)
        success, kmeans_cluster=kmeans_check_cluster(kmeans_cluster, centroid,
                                                     input_data_index, num_cluster)

    return centroid, kmeans_cluster


def cs_rs(temp_data_index):
    #: output a dictionary, for each key value pair, the key is the number of index
    # and value is (centroid, (s,sum,sumqs))
    output_rs={}
    output_cs={}
    output_cs_data_index={}
    for i in temp_data_index:
        if len(temp_data_index[i])==1:
            output_rs[temp_data_index[i][0]]=dic_data[temp_data_index[i][0]]
            # output_rs.append([temp_data_index, n_sum_sumsq(temp_data_index)])
        else:
            output_cs[tuple(centroid(temp_data_index[i]))]=[i,n_sum_sumsq(temp_data_index[i])]
            output_cs_data_index[i]=temp_data_index[i]
            # output_cs.append([centroid(temp_data_index[i]), n_sum_sumsq(temp_data_index[i])])
    return output_cs, output_cs_data_index, output_rs

def mahalanobis_distance(input_data_, centroid, stats_information):
    # Calculate mahalanobis distance between one data point and one cluster's centroid
    d=0

    if type(input_data_)!=list:


        input_data=dic_data[input_data_]
    else:

        input_data=input_data_.copy()
    centroid_list=list(centroid)
    for i in range(len(input_data)):
        # print(centroid)
        # print(stats_information[2],1)
        # print((stats_information[1]),2)
        variance=stats_information[2][i]-(stats_information[1][i]**2)
        if variance==0:
            d+=(input_data[i]-centroid_list[i])**2
        else:
            d+=(input_data[i]-centroid_list[i])**2/variance
    return d**0.5

def updata_ds_index_cs_rs(input_data_point_index):
    for i in ds_stats:
        if mahalanobis_distance(input_data_point_index, i, ds_stats[i][1])<a*(dimension**(1/2)):
            ds_data_index[ds_stats[i][0]].append(input_data_point_index)
            return
    for i in cs_stats:
        if mahalanobis_distance(input_data_point_index, i, cs_stats[i][1]) < a * (dimension ** (1 / 2)):
            cs_data_index[cs_stats[i][0]].append(input_data_point_index)
            return
    rs[input_data_point_index]=dic_data[input_data_point_index]
    return

def merge_cs_1(cs_stats, cs_data_index):
    cs_stats_new={}
    cs_data_index_new={}
    # rs_new={}
    temp=list(cs_stats.keys())
    processed=[]
    counter=0
    for i in range(len(temp)-1):
        for j in range(i+1, len(temp)):
            if mahalanobis_distance(list(temp[i]), list(temp[j]), cs_stats[temp[j]][1]) < a * (dimension ** (1 / 2))  or \
                mahalanobis_distance(list(temp[j]), list(temp[i]), cs_stats[temp[i]][1]) < a * (dimension ** (1 / 2)):
                if temp[i] not in processed and temp[j] not in processed:
                    processed.append(temp[i])
                    processed.append(temp[j])
                    cs_data_index_new[counter]=cs_data_index[cs_stats[temp[j]][0]]+cs_data_index[cs_stats[temp[i]][0]]
                    cs_stats_new[tuple(centroid([temp[i], temp[j]]))]=[counter, [cs_stats[temp[i]][1][0]+cs_stats[temp[j]][1][0],
                                                        [(cs_stats[temp[i]][1][1][t]*cs_stats[temp[i]][1][0]+cs_stats[temp[j]][1][1][t]*cs_stats[temp[j]][1][0])/\
                                                         (cs_stats[temp[i]][1][0]+cs_stats[temp[j]][1][0]) for t in range(dimension)],
                                                                [(cs_stats[temp[i]][1][2][t] * cs_stats[temp[i]][1][0] +
                                                                  cs_stats[temp[j]][1][2][t] * cs_stats[temp[j]][1][0])/\
                                                                 (cs_stats[temp[i]][1][0] + cs_stats[temp[j]][1][0]) for t in range(dimension)]]]
                    counter+=1
    for i in cs_stats:
        if i not in processed:
            cs_data_index_new[counter] = cs_data_index[cs_stats[i][0]]
            # print(len(cs_stats[i][1]))
            cs_stats_new[i]=[counter, cs_stats[i][1]]
            counter+=1
    return cs_stats_new, cs_data_index_new

def rs_ds_merge():
    if rs:
        for i in rs:
            for j in ds_stats:
                if mahalanobis_distance(rs[i], list(j), ds_stats[j][1]) < a * (dimension ** (1 / 2)):
                    ds_data_index[ds_stats[j][0]].append(i)
                    break
    # cs_stats={}
    # for i in cs_data_index:
    #     cs_stats[tuple(centroid(cs_data_index[i]))]=[i, ]



def ds_cs_merge():
    for i in cs_stats:
        for j in ds_stats:
            if mahalanobis_distance(list(i), list(j), ds_stats[j][1]) < a * (dimension ** (1 / 2))  or \
                mahalanobis_distance(list(j), list(i), cs_stats[i][1]) < a * (dimension ** (1 / 2)):
                ds_data_index[ds_stats[j][0]]+=cs_data_index[cs_stats[i][0]]
                del cs_data_index[cs_stats[i][0]]
                break
    cs_stats.clear()
    for i in cs_data_index:
        # cs_stats[tuple(centroid(cs_data_index[i]))] = [i, n_sum_sumsq(cs_data_index[i])]
        for j in cs_data_index[i]:
            if j not in rs:
                rs[j]=dic_data[j]
    cs_data_index.clear()



# input_path = 'test1'
flag=0
a=4
# start=time.time()
inter_data=[]
num_round=0
for filename in os.listdir(input_path):
# for filename in ["data0.txt","data1.txt","data2.txt","data3.txt","data4.txt",
#                  "data5.txt","data6.txt","data7.txt","data8.txt","data9.txt"]:
    num_round+=1
    if not flag:
        flag=1
        with open(input_path+'/'+filename, "r") as f:
            # flag=1
            data=read_txt_file(f)
            dic_data, dimension=data_dic(data)
            data_index=list(dic_data.keys())
            random.shuffle(data_index)
            sample_index=data_index[:int(0.02*len(data_index))]
            c, ds_data_index=kmeans(sample_index, num_clusters)
            ds_stats={}
            for i in ds_data_index:
                ds_stats[tuple(centroid(ds_data_index[i]))]=[i, n_sum_sumsq(ds_data_index[i])]
            temp_c, temp_data_index=kmeans(data_index[int(0.02*len(data_index)):], 5*num_clusters)
            cs_stats, cs_data_index, rs = cs_rs(temp_data_index)
            # inter_data.append([str(num_round), str(10), str(sum([len(ds_data_index[i]) for i in ds_data_index])), str(len(cs_data_index)),
            #                    str(sum([len(cs_data_index[i]) for i in cs_data_index])), str(len(rs))])
            # num_round+=1

    else:
        with open(input_path + '/' + filename, "r") as f:
            data = read_txt_file(f)
            dic_data, dimension = data_dic(data)
            # print(len(dic_data))
            # data_index = list(dic_data.keys())
            for i in dic_data:
                updata_ds_index_cs_rs(i)
            cs_stats, cs_data_index = merge_cs_1(cs_stats, cs_data_index)

    temp_cs_info=str(sum([len(cs_data_index[i]) for i in cs_data_index]))
    temp_rs_info=str(len(rs))
    ds_cs_merge()
    inter_data.append([str(num_round), str(num_clusters), str(sum([len(ds_data_index[i]) for i in ds_data_index])), str(len(cs_data_index)),
                       temp_cs_info, temp_rs_info])



ds_cs_merge()
rs_ds_merge()

output_dic={}
for i in ds_data_index:
    for t in ds_data_index[i]:
        output_dic[str(int(t))]=i

for i in cs_data_index:
    for t in cs_data_index[i]:
        output_dic[str(int(t))]=-1

for i in rs:
    output_dic[str(int(i))] = -1


s = json.dumps(output_dic)
open(result_output_file,"w").write(s)


with open(intermediate_file_path, mode='w') as inter:
    writer = csv.writer(inter, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['round_id', "nof_cluster_discard", 'nof_point_discard', "nof_cluster_compression", 'nof_point_compression', 'nof_point_retained'])

    for i in inter_data:
        writer.writerow(i)
