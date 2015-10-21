from __future__ import absolute_import
from celery import Celery
import os
import sys
import urllib2
from datetime import datetime
import swiftclient.client
import json
from flask import Flask, jsonify
from mycelery import app
import time


'''app = Celery('tasks', backend='rpc://', broker='amqp://')'''


@app.task
def add(x,y):
    return x+y

@app.task
def connect(filenumber,ignore):
    start_time = time.time()
    alltweets = 0
    hancount = 0
    honcount = 0
    dencount = 0
    detcount = 0
    dennacount = 0
    dennecount = 0
    hencount = 0
    words = ["han","hon","den","det","denna","denne","hen"]


    config = {'user':'*****',
              'key':'******',
              'tenant_name':'ACC-Course',
              'authurl':'http://smog.uppmax.uu.se:5000/v2.0'}

    conn = swiftclient.client.Connection(auth_version=2, **config)

    (response, bucket_list) = conn.get_account()

    objectlist = []
    (response, obj_list) = conn.get_container('tweets')
    for obj in obj_list:
        objectname = obj['name']
        if objectname.endswith('.txt'):
                 objectlist.append(objectname)
                 
    count = len(objectlist)

        
    data_file = urllib2.urlopen('http://smog.uppmax.uu.se:8080/swift/v1/tweets/tweets_'+str(filenumber)+'.txt')


    for line in data_file: 
                    if len(line.strip())==0:
                       continue
                    tweet_object = json.loads(line)
                    alltweets=alltweets+1
                                    # filter retweets
                    try:
                        if tweet_object['retweeted_status'] == "true":
                                      continue
                                 # search word
                        for word in words:
                                            if word in tweet_object["text"]:
                                                if word=='han' :
                                                            hancount=hancount+1
                                                if word=='hon' :
                                                            honcount=honcount+1
                                                if word=='den' :
                                                            dencount=dencount+1
                                                if word=='det' :
                                                            detcount=detcount+1
                                                if word=='denna' :
                                                            dennacount=dennacount+1
                                                if word=='denne' :

                                                            dennecount=dennecount+1
                                                if word=='hen' :
                                                            hencount=hencount+1
                    except KeyError,e:
                        continue
 
    print 'hancount is' + str(hancount)
    print 'honcount is' + str(honcount)
    print 'dencount is' + str(dencount)
    print 'detcount is' + str(detcount)
    print 'dennacount is' + str(dennacount)
    print 'dennecount is' + str(dennecount)
    print 'hencount is' + str(hencount)

    data = {"han": hancount,
        "hon": honcount,
        "den": dencount,
        "det": detcount,
        "denna": dennacount,
        "denne": dennecount,
        "hen": hencount,
        "alltweets": alltweets}
    elapsed_time = time.time() - start_time
    print elapsed_time
    return elapsed_time
    """ with open('data.txt','w') as outfile:
        json.dump(data,outfile) """ 

