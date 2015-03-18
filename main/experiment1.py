"""
This experiment is set up in the following way:
1) We choose n users (set U) from k subreddits (set R) listed who have written at least c characters in each r
2) We train n*k models for authorship; i.e. for each user u in U, we gather a sample of text of size c for u from R
3) We test each document d from D_r with all n*k models
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

def train_classifiers(user):
    global args
    corpus = RedditMySQLCorpus()
    corpus.setup(host='localhost', user='root', db='reddit')
    cls = {}
    for sr in args.subreddits:
        document = corpus.get_train_documents('comment', user['username'], sr, args.c[0]).encode('utf-8')
        cl = RedditPPM()
        cl.train(document)
        cls[sr] = cl
    return cls


def test_classifiers(atuple):
    global userlist
    D, cls, sr1, u, sr2 = atuple

    result = []
    ranklist = []
    for d in D:
        if d['username'] is None or d['username'] != u['username']:
            continue
        for user in userlist:
            username = user['username']
            cl = cls[username]
            score = cl.score(d['text'])
            result.append({'username': username, 'score': score})
        result = sorted(result, key=(itemgetter('score')))
        rank = 0
        for i in range(0, len(result)):
            if result[i]['username'] == u['username']:
                rank = i
                break
        ranklist.append(rank)
    return ranklist

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', choices=['submission', 'comment'], required=True)
    parser.add_argument('n', type=int, nargs=1)
    parser.add_argument('c', type=int, nargs=1)
    parser.add_argument('subreddits', type=str, nargs='+')
    args = parser.parse_args(sys.argv[1:])

    corpus = RedditMySQLCorpus(multiprocessing.cpu_count())
    corpus.setup(host='localhost', user='root', db='reddit')
    corpus.create()
    print(args.n, args.c, args.subreddits)
    userlist = corpus.get_user_list('comment', args.c[0], args.subreddits)
    userlist = userlist[:args.n[0]]

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    result0 = pool.map(train_classifiers, userlist)

    cls = {}
    corpora = {}
    for sr in args.subreddits:
        cls[sr] = {}
        corpora[sr] = corpus.get_test_documents('comment', sr)

    for i in range(0, len(result0)):
        for sr in args.subreddits:
            cl = result0[i][sr]
            cls[sr][userlist[i]['username']] = cl

    pairings = [ (corpora[sr2], cls[sr1], sr1, u, sr2) for sr1 in args.subreddits for u in userlist for sr2 in args.subreddits ]

    result1 = pool.map(test_classifiers, pairings)

    for i in range(0, len(pairings)):
        pairing = pairings[i]
        res = result1[i]
        D, cl, sr1, u, sr2 = pairing
        median = numpy.median(res)
        print('%s-%s on %s avg rank: %f' % (sr1, u['username'], sr2, median))
