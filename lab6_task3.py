import tweepy
import random
import sys

fake_port=int(sys.argv[1])


output_path = sys.argv[2]

num_sample = 100

flag = 1

sequence_num = 0

tags = {}

t = []

letters='qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM'

# with open(output_path,'w',encoding='utf-8-sig') as csvfile:

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):

        global sequence_num
        global flag

        # print(status.entities["hashtags"])

        tags_list = status.entities["hashtags"]
        english=1
        if tags_list:

            for i in tags_list:
                for tt in i['text']:
                    if tt not in letters:
                        english=0
                        break
                if not english:
                    break



            if english:
                sequence_num += 1

                if sequence_num > num_sample:
                    if random.randint(1, sequence_num) <= num_sample:
                        flag2 = random.randint(0, 99)
                        tw_remove = t[flag2]
                        t[flag2] = status
                        tw_tag_remove = tw_remove.entities["hashtags"]
                        for i in tw_tag_remove:
                            temp = i["text"]
                            if temp in tags:
                                tags[temp] -= 1
                                if tags[temp] == 0:
                                    del tags[temp]
                        for i in tags_list:
                            if i["text"] not in tags:
                                tags[i["text"]] = 1
                            else:
                                tags[i["text"]] += 1
                        t[flag2] = status


                else:
                    t.append(status)

                    for i in tags_list:
                        if i["text"] not in tags:
                            tags[i["text"]] = 1
                        else:
                            tags[i["text"]] += 1

                current_tags_status_list = []
                for i in tags:
                    # print(tags)
                    current_tags_status_list.append([i, tags[i]])

                current_tags_status_list.sort(key=lambda x: (-x[1], x[0]))
                counter = 1
                top = []
                # with open(output_path, 'w') as csvfile:
                if flag == 1:
                    flag = 0
                    with open(output_path, 'w', encoding='utf-8-sig') as csvfile:
                        csvfile.write("The number of tweets with tags from the beginning:" + str(sequence_num))
                        if len(current_tags_status_list) == 1:
                            csvfile.write(
                                "\n" + str(current_tags_status_list[0][0]) + ':' + str(current_tags_status_list[0][1]))
                            top.append(current_tags_status_list[0][1])
                        else:
                            csvfile.write(
                                "\n" + str(current_tags_status_list[0][0]) + ':' + str(current_tags_status_list[0][1]))
                            top.append(current_tags_status_list[0][1])
                            while len(top) < 3 and counter < len(current_tags_status_list):
                                if current_tags_status_list[counter][1] < current_tags_status_list[counter - 1][1] and len(
                                        top) < 3:
                                    top.append(current_tags_status_list[counter][1])
                                    csvfile.write("\n" + str(current_tags_status_list[counter][0]) + ':' + str(
                                        current_tags_status_list[counter][1]))
                                else:
                                    if current_tags_status_list[counter][1] == current_tags_status_list[counter - 1][1]:
                                        csvfile.write("\n" + str(current_tags_status_list[counter][0]) + ':' + str(
                                            current_tags_status_list[counter][1]))
                                counter += 1
                        csvfile.write("\n")
                        csvfile.write("\n")
                else:
                    with open(output_path, 'a', encoding='utf-8-sig') as csvfile:
                        csvfile.write("The number of tweets with tags from the beginning:" + str(sequence_num))
                        if len(current_tags_status_list) == 1:
                            csvfile.write(
                                "\n" + str(current_tags_status_list[0][0]) + ':' + str(current_tags_status_list[0][1]))
                            top.append(current_tags_status_list[0][1])
                        else:
                            csvfile.write(
                                "\n" + str(current_tags_status_list[0][0]) + ':' + str(current_tags_status_list[0][1]))
                            top.append(current_tags_status_list[0][1])
                            while len(top) < 3 and counter < len(current_tags_status_list):
                                if current_tags_status_list[counter][1] < current_tags_status_list[counter - 1][1] and len(
                                        top) < 3:
                                    top.append(current_tags_status_list[counter][1])
                                    csvfile.write("\n" + str(current_tags_status_list[counter][0]) + ':' + str(
                                        current_tags_status_list[counter][1]))
                                else:
                                    if current_tags_status_list[counter][1] == current_tags_status_list[counter - 1][1]:
                                        csvfile.write("\n" + str(current_tags_status_list[counter][0]) + ':' + str(
                                            current_tags_status_list[counter][1]))
                                counter += 1
                        csvfile.write("\n")
                        csvfile.write("\n")




akey= "k3WqMoP1Vy6QdKQOFO14MEBlm"
aksecret= "8fFCnosOrW9bIy8rbxAHrB9mGomtfWiCcIIFuPMCT355yvGtbO"
token= "1345126940-saQ6YwQCfRRCPACPHofXVMwjabcMczSRDjUIhvk"
atoken= "0JWPCu6TJ0nFZFlBcu4pBg4wTffTnlt1HwBQcUeqKIU3M"
auth = tweepy.OAuthHandler(akey, aksecret)
auth.set_access_token(token, atoken)

api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

myStream.filter(track=['#'])