#!/usr/bin/env python3

import json
from datetime import datetime
from datetime import timedelta
import numpy as np
import pandas as pd
import networkx as nx
import itertools
import sys

def tweets_to_average_degree(tweets_in_window):

    if tweets_in_window.shape[0] == 0:
        return 0

    edges = []
    for t in tweets_in_window.hashtag:
        edges += list(itertools.combinations(t, 2))
        # print(edges)

    G = nx.Graph()
    G.add_edges_from(edges)

    # a list of the degree for each node
    degrees = [G.degree(node) for node in G.nodes()]
    return np.mean(degrees)



f_in = sys.argv[1]
f_out = sys.argv[2]
f_out_handle = open(f_out, 'w')
print('input file: {0}    output file: {1}'.format(f_in, f_out))


# tweets in 60s window (only record 'created_at' and 'hashtag')
tweets_in_window = pd.DataFrame({'created_at': [],
                                 'hashtag': []})
max_time = datetime.min
ii = 0
for line in open(f_in):
    # if ii > 200:
    #     break
    # print('message {}'.format(ii))

    new_tweet = json.loads(line)
    try:
        created_at, hashtag = new_tweet['created_at'], new_tweet['entities']['hashtags']
    except: # for rate-limit message
        # print('time and hashtag not found')
        continue
    # ii += 1

    created_at = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
    hashtag = [element['text'] for element in hashtag]

    max_time = max(max_time, created_at)

    # only record tweets w/ >= 2 hashtags, since only they contribute to graph
    # tweets w/ < 2 hashtags only contribute to update max_time
    if len(hashtag) >= 2:
        new_tweet_row = pd.DataFrame({'created_at': [created_at],
                                      'hashtag': [hashtag]})
        # print("created at: {c}, hastag = {h}".format(c=created_at, h=hashtag))
        # print(new_tweet_row)
        tweets_in_window = tweets_in_window.append(new_tweet_row, ignore_index=True)

    tweets_in_window = tweets_in_window[tweets_in_window.created_at > max_time - timedelta(seconds=60)]
    # not relying on the tweets to be in order
    # if new tweet is out of order and >60s old, it's removed in this step

    # print(max(tweets_in_window.created_at) - min(tweets_in_window.created_at))
    # # why max time difference isn't 59s?
    average_degree = tweets_to_average_degree(tweets_in_window)
    print("%.2f" % average_degree, file=f_out_handle)

f_out_handle.close()

print('Completed.')