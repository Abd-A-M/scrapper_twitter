import re
import subprocess, sys
import time

import pymongo
import schedule

'''
:param tweet object.
:return and print the tweet object data
'''

client = pymongo.MongoClient("mongodb://10.10.10.25/")
db = client["DB_abdullah"]
table1 = db["basem"]


def splitter(line):
    items_ = line.split(" ", maxsplit=5)
    TID = items_[0]
    TDate = items_[1]
    TTime = items_[2]

    Tuser = items_[4]
    TText = items_[5]
    url = ""
    try:
        url = re.findall(r'(https?://\S+)', TText['TText'])
    except:
        url = ""
    element = {"TID": TID, "TDate": TDate, "TTime": TTime,  "Tuser": Tuser, "TText": TText,"url":url}
    table1.insert_one(element)

    for item in items_:
        print(str(item))



'''
:param user name to collect all tweet in their timeline (string).
:param Tweettype:,"--images","--videos","--media" or empty
:return tweet object
'''


def run_commands(command):
    command = command.split("[separator]")
    print(command)
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,
                         text=True)

    return p


def user_timeline(username, tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""
    if tweettype.isspace():
        command = "twint" + "[separator]" + "-u" + "[separator]" + username + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-u" + "[separator]" + username + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp

    p = run_commands(command)
    lines = p.stdout

    for line in lines:
        splitter(line)


'''
:param user name to collect all tweet and its replies (string).
:param Tweettype:,"--images","--videos","--media" or empty
:return tweet object
'''


def user_timeline_with_replies(username, tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""
    if tweettype.isspace():
        command = "twint" + "[separator]" + "-u" + "[separator]" + username + "[separator]" + "--timeline" + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-u" + "[separator]" + username + "[separator]" + "--timeline" + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp

    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


'''
:param user name to collect all tweet with Keyword in their timeline  (string).
:param Tweettype:,"--images","--videos","--media" or empty
:return tweet object
'''


def user_timeline_keyword(username, keyword, tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""
    if tweettype.isspace():
        command = "twint" + "[separator]" + "-u" + "[separator]" + username + "[separator]" + "-s" + "[separator]" + keyword + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-u" + "[separator]" + username + "[separator]" + "-s" + "[separator]" + keyword + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp
    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


'''
:param user name to collect all tweet with Keyword from anyone's timeline  (string).
:param Tweettype:,"--images","--videos","--media" or empty
:return tweet object
'''
''''

 


'''


def everyone_timeline_keyword(keyword="باسم عوض الله", tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""
    if tweettype.isspace():
        command = "twint" + "[separator]" + "-s" + "[separator]" + keyword + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-s" + "[separator]" + keyword + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp

    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


'''
:param user name to collect all tweet  from user's timeline, start collect from specified date (string).
:paarm Tweettype:,"--images","--videos","--media" or empty
:return tweet object
'''


def user_timeline_since(username, date, tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""
    if tweettype.isspace():
        command = "twint" + "[separator]" + "-u" + "[separator]" + username + "[separator]" + "--since" + "[separator]" + date + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-u" + "[separator]" + username + "[separator]" + "--since" + "[separator]" + date + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp
    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


'''
:param place coordinates and distance.Tweettype:,"--images","--videos","--media" or empty
:return tweet object, search based on country and distance
'''


def tweets_around_place(X_coordinates, Y_coordinates, distance, tweettype=" ", min_like=0, min_retweets=0,
                        min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""
    if tweettype.isspace():
        command = "twint" + "[separator]" + "-g=" + X_coordinates + "," + Y_coordinates + "," + str(
            distance) + "km" + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-g=" + X_coordinates + "," + Y_coordinates + "," + str(
            distance) + "km" + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp

    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


'''
:param place city name.Tweettype:,"--images","--videos","--media" or empty
:return tweet object, search based on city name
'''


def tweets_around_city(city, tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""
    if tweettype.isspace():
        command = "twint" + "[separator]" + "--near" + "[separator]" + "'" + city + "'" + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "--near" + "[separator]" + "'" + city + "'" + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp

    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


'''
:param place keyword and date. Tweettype:,"--images","--videos","--media" or empty
:return tweet object, search based on two inputs
'''


def tweets_keyword_untildate(keyword, date, tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""
    if tweettype.isspace():
        command = "twint" + "[separator]" + "-s" + "[separator]" + keyword + "[separator]" + "--until" + "[separator]" + date + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-s" + "[separator]" + keyword + "[separator]" + "--until" + "[separator]" + date + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp
    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


'''
:param place keyword and date. Tweettype:,"--images","--videos","--media" or empty
:return tweet object, search based on two inputs
'''


def tweets_keyword_sincedate(keyword, date, tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""

    if tweettype.isspace():
        command = "twint" + "[separator]" + "-s" + "[separator]" + keyword + "[separator]" + "--since" + "[separator]" + date + "[separator]" + "--count" + min_l + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-s" + "[separator]" + keyword + "[separator]" + "--since" + "[separator]" + date + "[separator]" + "--count" + "[separator]" + tweettype + min_l + min_rt + min_rp
    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


'''
:param place keyword ,Tweettype:,"--images","--videos","--media" or empty
:return tweet object, search based on keyword in verified accounts.
'''


def tweets_keyword_verifiedaccount(keyword, tweettype=" ", min_like=0, min_retweets=0, min_replies=0):
    if min_like != 0:
        min_l = "[separator]" "--min-like" + "[separator]" + str(min_like)
    else:
        min_l = ""

    if min_retweets != 0:
        min_rt = "[separator]" "--min-retweets" + "[separator]" + str(min_retweets)
    else:
        min_rt = ""

    if min_replies != 0:
        min_rp = "[separator]" "--min-replies" + "[separator]" + str(min_replies)
    else:
        min_rp = ""

    if tweettype.isspace() or not tweettype:
        command = "twint" + "[separator]" + "-s" + "[separator]" + "'" + keyword + "'" + "[separator]" + "--verified" + "[separator]" + "--count" + str(
            min_l) + min_rt + min_rp
    else:
        command = "twint" + "[separator]" + "-s" + "[separator]" + "'" + keyword + "'" + "[separator]" + "--verified" + "[separator]" + "--count" + "[separator]" + tweettype + str(
            min_l) + min_rt + min_rp

    p = run_commands(command)
    lines = p.stdout
    for line in lines:
        splitter(line)


# import twint
#
# # Configure
# c = twint.Config()
# c.Search = "covid"
# c.Limit = 10
# c.Hide_output=False
# c.Store_csv = True
# c.Output = "test.csv"
# # Run
# twint.run.Search(c)

# everyone_timeline_keyword()
# user_timeline("Zendayagraphics","--images",5,5)
# user_timeline_keyword("Zendayagraphics","we")
# schedule.every().hour.do(everyone_timeline_keyword)
# user_timeline_since("Zendayagraphics", "2015-12-20")
# tweets_around_place("48.880048", "2.385939", 1)
# tweets_keyword_untildate("KADDB","2015-12-20")
# tweets_keyword_sincedate("KADDB","2015-12-20","--images",5,5,5)
# tweets_keyword_verifiedaccount("KADDB","",0,0,0)
# user_timeline_with_replies("Zendayagraphics")

# while True:
#     try:
#         schedule.run_pending()
#     except:
#         pass
#     time.sleep(500)
