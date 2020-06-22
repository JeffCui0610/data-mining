from pyspark import SparkContext
SparkContext.setSystemProperty("spark.driver.memory", "4g")
sc=SparkContext()
import sys

threshold=int(sys.argv[1])
input_file_path=sys.argv[2]
betweeness_output_file_path=sys.argv[3]
community_output_file_path=sys.argv[4]
# threshold=7

inputRawData = sc.textFile(input_file_path).map(lambda line: line.split(","))
user_businesslist = inputRawData.groupByKey().map(lambda x: (x[0], list(x[1]))).filter(
    lambda element: element[0] != 'user_id')

all_user=list(sorted(user_businesslist.map(lambda element:element[0]).collect()))
user_code_dic={}
counter=0
for i in all_user:
    if i not in user_code_dic:
        user_code_dic[i]=str(counter)
        counter+=1

user_businesslist_dict=user_businesslist.collectAsMap()

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

# edge=list(map(lambda e:(user_code_dic[e[0]], user_code_dic[e[1]]),select_pair(all_user)))
edge=list(map(lambda e:(e[0], e[1]),select_pair(all_user)))
m=len(edge)
graph=sc.parallelize(edge).map(lambda element:(element, (element[1], element[0]))).flatMap(lambda e:e)\
    .groupByKey().map(lambda element:(element[0], list(element[1]))).collectAsMap()

score_table_node_0=sc.parallelize(edge).map(lambda element:(element, (element[1], element[0]))).flatMap(lambda e:e)\
    .groupByKey().map(lambda element:(element[0],0)).collectAsMap()
node_list=sc.parallelize(edge).map(lambda element:(element, (element[1], element[0]))).flatMap(lambda e:e)\
    .groupByKey().map(lambda element:(element[0])).collect()


#input: original graph
#output: Community in graph
# def devide_subgraph(graph):
#     def Diff(li1, li2):
#         li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
#         return li_dif
#
#     def layer_bfs(graph, node):
#         visited = [node]
#         queue = [node]
#         while queue:
#             s = queue.pop(0)
#             for i in graph[s]:
#                 if i not in visited:
#                     visited.append(i)
#                     queue.append(i)
#         return visited
#
#     def construct_sub_graph(input_list):
#         output = {}
#         for i in input_list:
#             if i not in output:
#                 output[i] = graph[i]
#         return output
#
#     graph_list = []
#     aaa = layer_bfs(graph, node_list[0])
#     counter = aaa.copy()
#     graph_list.append(construct_sub_graph(aaa))
#     while True:
#         ttt = Diff(counter, node_list)
#         aaa = layer_bfs(graph, ttt[0])
#         graph_list.append(construct_sub_graph(aaa))
#         counter += aaa
#         if len(counter) >= len(node_list):
#             break
#     return graph_list

score_table_edge_0={}
for i in edge:
    if i not in score_table_edge_0:
        score_table_edge_0[i]=0
edge_score_table=score_table_edge_0.copy()


#Input: one graph and one node as root
#Output: The betweeness under input situation
def help1(node, graph1,  score_table_node_0, score_table_edge_0):

    def layer_bfs(graph, node):#Output
        visited=[node]
        queue=[node]
        node_layer={node:0}

        while queue:
            s=queue.pop(0)

            for i in graph[s]:
                if i not in visited:
                    if i not in node_layer:
                        node_layer[i]=node_layer[s]+1
                    visited.append(i)
                    queue.append(i)
        return node_layer
    dd = layer_bfs(graph1, node)

    layer_node = {}
    for i in dd:
        if dd[i] not in layer_node:
            layer_node[dd[i]] = [i]
        else:
            layer_node[dd[i]].append(i)


    score_table_edge_blank=score_table_edge_0.copy()
    score_table_node_blank=score_table_node_0.copy()

    graph2=graph1.copy()
    graph=graph1.copy()
    for i in graph2:
        layer=dd[i]
        select_layer=[]
        for t in graph2[i]:
            if t not in layer_node[layer]:
                select_layer.append(t)
        graph[i]=select_layer
    print(222)
    def bfs(node, graph):
        leaves_list = []
        visited = []
        queue = []
        done = []
        visited.append(node)
        queue.append(node)

        while queue:
            s = queue.pop(0)
            done.append(s)

            leaves = True

            for i in graph[s]:
                if i not in done:
                    leaves = False

            for neightbors in graph[s]:
                if neightbors not in visited:
                    visited.append(neightbors)
                    queue.append(neightbors)
                    # leaves=False
            if leaves:
                leaves_list.append(s)
        return leaves_list, visited

    leaves_list, visited = bfs(node,graph)

    def help2(input_pair):
        if input_pair in score_table_edge_blank:
            return input_pair
        else:
            return tuple(reversed(input_pair))

    calculated=[]

    for i in reversed(visited):
        if i in leaves_list:
            calculated.append(i)
            num_neighbors = len(graph[i])
            contribution=1/num_neighbors
            for t in graph[i]:
                score_table_node_blank[t] += 1 / num_neighbors
                score_table_edge_blank[help2((i,t))]+=contribution
        else:
            score_table_node_blank[i] += 1
            calculated.append(i)
            num_neighbors=0
            not_cal=[]
            for t in graph[i]:
                if t not in calculated:
                    not_cal.append(t)
                    num_neighbors+=1
            if num_neighbors==0:
                break
            for t in not_cal:
                score_table_node_blank[t] += score_table_node_blank[i] / num_neighbors
                score_table_edge_blank[help2((i, t))] += score_table_node_blank[i] / num_neighbors
    return score_table_edge_blank

score_table_edge_01=score_table_edge_0.copy()
score_table_node_01=score_table_node_0.copy()



#Input: One whole graph
#Output: The final betweeness
def betweeness_whole_graph(graph):
    def devide_subgraph(graph):
        def Diff(li1, li2):
            li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
            return li_dif

        def layer_bfs(graph, node):
            visited = [node]
            queue = [node]
            while queue:
                s = queue.pop(0)
                for i in graph[s]:
                    if i not in visited:
                        visited.append(i)
                        queue.append(i)
            return visited

        def construct_sub_graph(input_list):
            output = {}
            for i in input_list:
                if i not in output:
                    output[i] = graph[i]
            return output

        graph_list = []
        aaa = layer_bfs(graph, node_list[0])
        counter = aaa.copy()
        graph_list.append(construct_sub_graph(aaa))
        while True:
            ttt = Diff(counter, node_list)
            if not ttt:
                return graph_list
            aaa = layer_bfs(graph, ttt[0])
            graph_list.append(construct_sub_graph(aaa))
            counter += aaa
            if len(counter) >= len(node_list):
                break
        return graph_list
    graph_list=devide_subgraph(graph)

    # def help1(node, graph1, score_table_node_0, score_table_edge_0):
    #
    #     def layer_bfs(graph, node):  # Output
    #         visited = [node]
    #         queue = [node]
    #         node_layer = {node: 0}
    #
    #         while queue:
    #             s = queue.pop(0)
    #
    #             for i in graph[s]:
    #                 if i not in visited:
    #                     if i not in node_layer:
    #                         node_layer[i] = node_layer[s] + 1
    #                     visited.append(i)
    #                     queue.append(i)
    #         return node_layer
    #
    #     dd = layer_bfs(graph1, node)
    #
    #     layer_node = {}
    #     for i in dd:
    #         if dd[i] not in layer_node:
    #             layer_node[dd[i]] = [i]
    #         else:
    #             layer_node[dd[i]].append(i)
    #
    #     score_table_edge_blank = score_table_edge_0.copy()
    #     score_table_node_blank = score_table_node_0.copy()
    #
    #     graph2 = graph1.copy()
    #     graph = graph1.copy()
    #     for i in graph2:
    #         layer = dd[i]
    #         select_layer = []
    #         for t in graph2[i]:
    #             if t not in layer_node[layer]:
    #                 select_layer.append(t)
    #         graph[i] = select_layer
    #
    #     def bfs(node, graph):
    #         leaves_list = []
    #         visited = []
    #         queue = []
    #         done = []
    #         visited.append(node)
    #         queue.append(node)
    #
    #         while queue:
    #             s = queue.pop(0)
    #             done.append(s)
    #
    #             leaves = True
    #
    #             for i in graph[s]:
    #                 if i not in done:
    #                     leaves = False
    #
    #             for neightbors in graph[s]:
    #                 if neightbors not in visited:
    #                     visited.append(neightbors)
    #                     queue.append(neightbors)
    #                     # leaves=False
    #             if leaves:
    #                 leaves_list.append(s)
    #         return leaves_list, visited
    #
    #     leaves_list, visited = bfs(node, graph)
    #
    #     def help2(input_pair):
    #         if input_pair in score_table_edge_blank:
    #             return input_pair
    #         else:
    #             return tuple(reversed(input_pair))
    #
    #     calculated = []
    #
    #     for i in reversed(visited):
    #         if i in leaves_list:
    #             calculated.append(i)
    #             num_neighbors = len(graph[i])
    #             if num_neighbors==0:
    #                 break
    #             contribution = 1 / num_neighbors
    #             for t in graph[i]:
    #                 score_table_node_blank[t] += 1 / num_neighbors
    #                 score_table_edge_blank[help2((i, t))] += contribution
    #         else:
    #             score_table_node_blank[i] += 1
    #             calculated.append(i)
    #             num_neighbors = 0
    #             not_cal = []
    #             for t in graph[i]:
    #                 if t not in calculated:
    #                     not_cal.append(t)
    #                     num_neighbors += 1
    #             if num_neighbors == 0:
    #                 break
    #             for t in not_cal:
    #                 score_table_node_blank[t] += score_table_node_blank[i] / num_neighbors
    #                 score_table_edge_blank[help2((i, t))] += score_table_node_blank[i] / num_neighbors
    #     return score_table_edge_blank
    def help1(node, graph1, score_table_node_0, score_table_edge_0):

        def layer_bfs(graph, node):  # Output
            visited = [node]
            queue = [node]
            node_layer = {node: 0}

            while queue:
                s = queue.pop(0)

                for i in graph[s]:
                    if i not in visited:
                        if i not in node_layer:
                            node_layer[i] = node_layer[s] + 1
                        visited.append(i)
                        queue.append(i)
            return node_layer

        dd = layer_bfs(graph1, node)

        layer_node = {}
        for i in dd:
            if dd[i] not in layer_node:
                layer_node[dd[i]] = [i]
            else:
                layer_node[dd[i]].append(i)

        score_table_edge_blank = score_table_edge_0.copy()
        score_table_node_blank = score_table_node_0.copy()

        graph2 = graph1.copy()
        graph = graph1.copy()
        for i in graph2:
            layer = dd[i]
            select_layer = []
            for t in graph2[i]:
                if t not in layer_node[layer]:
                    select_layer.append(t)
            graph[i] = select_layer

        def bfs(node, graph):
            leaves_list = []
            visited = []
            queue = []
            done = []
            visited.append(node)
            queue.append(node)

            while queue:
                s = queue.pop(0)
                done.append(s)

                leaves = True

                for i in graph[s]:
                    if i not in done:
                        leaves = False

                for neightbors in graph[s]:
                    if neightbors not in visited:
                        visited.append(neightbors)
                        queue.append(neightbors)
                        # leaves=False
                if leaves:
                    leaves_list.append(s)
            return leaves_list, visited





        def node_weights(node, graph):
            node_weights={node:1}
            visited=[node]
            queue=[node]
            while queue:
                s=queue.pop(0)

                for neighbors in graph[s]:
                    if neighbors not in visited:
                        # sor = []
                        # for t in graph[neighbors]:
                        #     if t in visited:
                        #         sor.append(node_weights[t])
                        # weight=sor.count(min(sor))
                        weight=0
                        for idx in graph[neighbors]:
                            if idx in visited:
                                weight+=node_weights[idx]
                        node_weights[neighbors]=weight
                        visited.append(neighbors)
                        queue.append(neighbors)
            return node_weights


        node_weights=node_weights(node, graph)




        leaves_list, visited = bfs(node, graph)

        def help2(input_pair):
            if input_pair in score_table_edge_blank:
                return input_pair
            else:
                return tuple(reversed(input_pair))

        calculated = []

        def contribution_leaf(i,t):
            Sum=0
            for j in graph[i]:
                Sum+=node_weights[j]
            return node_weights[t]/Sum
        def contribution_nonleaves(i,t,non_cal):
            Sum=0
            for j in non_cal:
                Sum+=node_weights[j]
            return node_weights[t]/Sum
        for i in reversed(visited):
            if i in leaves_list:
                calculated.append(i)
                num_neighbors = len(graph[i])
                if num_neighbors==0:
                    break
                # contribution = 1 / num_neighbors
                for t in graph[i]:
                    score_table_node_blank[t] += contribution_leaf(i,t)
                    score_table_edge_blank[help2((i, t))] += contribution_leaf(i,t)
            else:
                score_table_node_blank[i] += 1
                calculated.append(i)
                num_neighbors = 0
                not_cal = []
                for t in graph[i]:
                    if t not in calculated:
                        not_cal.append(t)
                        num_neighbors += 1
                if num_neighbors == 0:
                    break
                for t in not_cal:
                    score_table_node_blank[t] += score_table_node_blank[i] * contribution_nonleaves(i,t,not_cal)
                    score_table_edge_blank[help2((i, t))] += score_table_node_blank[i] * contribution_nonleaves(i,t,not_cal)
        return score_table_edge_blank
    score_table_edge_01 = score_table_edge_0.copy()
    score_table_node_01 = score_table_node_0.copy()
    edge_score_table = score_table_edge_0.copy()
    for g in graph_list:
        for i in g:
            eee = help1(i, g, score_table_node_01, score_table_edge_01)
            for t in eee:
                edge_score_table[t] += eee[t]
    return edge_score_table, graph_list

#Input: dict contains betweeness
#Output: edge with max betnessess
def edge_max_between(input_dict):
    max_between=float("-inf")
    for i in input_dict:
        if input_dict[i]>max_between:
            max_between=input_dict[i]
            max_edge=i
    return max_edge, max_between

def cut_graph(previous_graph, max_edge):
    cut_graph=previous_graph.copy()
    # cut_graph[max_edge[0]]=previous_graph[max_edge[0]].remove(max_edge[1])
    # cut_graph[max_edge[1]]=cut_graph[max_edge[1]].remove(max_edge[0])
    cut_graph[max_edge[1]].remove(max_edge[0])
    cut_graph[max_edge[0]].remove(max_edge[1])
    return cut_graph

# adjust the order of edge pair
# def help2(input_pair):
#     if input_pair in score_table_edge_0:
#         return input_pair
#     else:
#         return tuple(reversed(input_pair))

def combination(input_list):
    output=[]
    for i in range(len(input_list)-1):
        for j in range(i+1, len(input_list)):
            output.append((input_list[i], input_list[j]))
    return output

delete=[]


def modularity(input_graph_list):
    q=0
    for i in input_graph_list:
        key_list=list(i.keys())
        if len(key_list)<=1:
            continue
        else:
            candidate_edge=combination(key_list)
            for t in candidate_edge:
                # k1=len(i[t[0]])
                # k2=len(i[t[1]])
                k1=k_dict[t[0]]
                k2 = k_dict[t[1]]
                # if (t in score_table_edge_0 or tuple(reversed(t)) in score_table_edge_0) and \
                #         (t not in delete and tuple(reversed(t)) not in delete):
                if (t in score_table_edge_0) or (tuple(reversed(t)) in score_table_edge_0):
                    q=q+2-2*(k1*k2/(2*m))
                else:
                    q=q-2*(k1*k2/(2*m))
    return q/(2*m)

#Question2

betweeness, graph_list=betweeness_whole_graph(graph)

def betweeness_output_format(input_dic):
    output_list=[]
    for i in input_dic:
        output_list.append((tuple(sorted(i)), input_dic[i]/2))
    output_list.sort(key=lambda x:((-x[1]), x[0][0]))
    return output_list

output=betweeness_output_format(betweeness)
output=list(map(lambda s:(s[0],',',' ',s[1]), output))
file1=open(betweeness_output_file_path, "w")
for i in output:
    file1.write(''.join(str(s) for s in i) + '\n')
file1.close()

k_dict={}
for i in graph_list:
    for t in i:
        k_dict[t]=len(i[t])




#This part for question3
# q_list=[]
# t_l=[]
# for i in range(1):
#     betweeness, graph_list=betweeness_whole_graph(graph)
#     q_list.append(modularity(graph_list))
#     max_edge,b=edge_max_between(betweeness)
#     # t_b.append(b)
#     t_l.append(len(graph_list))
#     graph=cut_graph(graph, max_edge)
validation=30
max_q=float("-inf")
while True:
    betweeness, graph_list = betweeness_whole_graph(graph)
    q=modularity(graph_list)
    if q>max_q:
        max_q=q
        best_graph=graph_list
        counter=0
    else:
        counter+=1
        if counter>validation:
            break
    max_edge, b = edge_max_between(betweeness)
    delete.append(max_edge)
    graph = cut_graph(graph, max_edge)


c_output=[]
for i in best_graph:
    t=[]
    for n in sorted(i):
        t.append('\''+str(n)+'\'')
        t.append(',')
        t.append(' ')
    t.pop()
    t.pop()
    c_output.append(t)
c_output.sort(key=lambda x:(len(x), x[0]))
file2=open(community_output_file_path, "w")
for i in range(len(c_output)-1):
    file2.write(''.join(str(s) for s in c_output[i]) + '\n')
file2.write(''.join(str(s) for s in c_output[-1])+ '\n')
file2.close()