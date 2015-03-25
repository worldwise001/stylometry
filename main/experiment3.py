"""
This experiment is set up in the following way:
1) We choose n users (set U) from k subreddits (set R) listed who have written at least c characters in each r
2) We train n*k models for authorship; i.e. for each user u in U, we gather a sample of text of size c for u from R
3) We test each document d from D_r with all n*k models (in this case documents from D_r are concatenated and divided into normalized chunks)
4) We rank each model trained a reddit r' for that document d, and record the position of the correct model
"""
__author__ = 'sharvey'
import argparse
import multiprocessing
from operator import itemgetter
import pprint
import numpy
import sys

from corpus.mysql.reddit import RedditMySQLCorpus
from classifiers.ppmc import RedditPPM
import graph

import cred

def train_classifiers(user):
    global args
    corpus = RedditMySQLCorpus()
    corpus.setup(**(cred.kwargs))
    cls = {}
    for sr in subreddits:
        document = corpus.get_train_documents(args.type, user['username'], sr, args.c[0]).encode('utf-8')
        cl = RedditPPM()
        cl.train(document)
        cls[sr] = cl
    del corpus
    return cls


def test_classifiers(atuple):
    global corpora
    global sr1
    global sr2
    global userlist
    global cls
    u1, u2 = atuple

    ranklist = []
    D = corpora[sr2][u2]
    if len(D) == 0:
        return None
    for d in D:
        result = []
        for user in userlist:
            cl = cls[sr1][user['username']]
            score = cl.score(d)
            result.append({'username': user['username'], 'score': score})
        result = sorted(result, key=(itemgetter('score')))
        rank = 0
        for i in range(0, len(result)):
            if result[i]['username'] == u1:
                rank = i
                break
        ranklist.append(rank)
    return ranklist

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', choices=['submission', 'comment'], required=True)
    parser.add_argument('n', type=int, nargs=1)
    parser.add_argument('c', type=int, nargs=1)
    args = parser.parse_args(sys.argv[1:])
    sr1 = 'worldnews'
    sr2 = 'gaming'
    subreddits = [ sr1, sr2 ]

    corpus = RedditMySQLCorpus(multiprocessing.cpu_count())
    corpus.setup(**(cred.kwargs))
    corpus.create()
    print(args.n, args.c, subreddits)
    userlist = corpus.get_user_list(args.type, args.c[0], subreddits)
    userlist = userlist[:args.n[0]]
    print('Got users')
    print('Downloading document list')
    corpora = {}
    for sr in subreddits:
        corpora[sr] = corpus.get_test_normalized_documents(args.type, sr, args.c[0])
        print('Downloaded %s (%d)' % (sr, len(corpora[sr])))
    del corpus
    maybe_users = list(corpora[sr2].keys())
    corpora_users = []
    for mu in maybe_users:
        if len(corpora[sr2][mu]) == 0:
            continue
        if len(corpora_users) >= 10:
            break
        corpora_users.append(mu)

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    print('Training classifiers')
    result0 = pool.map(train_classifiers, userlist)
    print('Trained')
    pool.close()
    pool.join()

    cls = {}
    for i in range(0, len(result0)):
        for sr in subreddits:
            if sr not in cls:
                cls[sr] = {}
            cl = result0[i][sr]
            cls[sr][userlist[i]['username']] = cl

    #pairings = [ (sr1, u, sr2)
    #             for sr1 in args.subreddits
    #             for u in corpora_users
    #             for sr2 in args.subreddits ]

    pairings = [ (train_user['username'], test_user) for train_user in userlist for test_user in corpora_users]

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    print('Testing classifiers')
    result1 = pool.map(test_classifiers, pairings)
    pool.close()
    pool.join()

    boxplot_data = {}
    for i in range(0, len(pairings)):
        pairing = pairings[i]
        res = result1[i]
        if res is None:
            continue
        u1, u2 = pairing
        if u1 not in boxplot_data:
            boxplot_data[u1] = {}
        boxplot_data[u1][u2] = res

    for u in boxplot_data:
        boxplot = boxplot_data[u]
        graph.boxplot_single('data/boxplot_%s' % u,
                             boxplot,
                             None,
                             (0, len(userlist)),
                             'tested user',
                             'mean rank'
                             'Box plot for mean rank for %s from worldnews to gaming' % u)
