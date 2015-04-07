__author__ = 'sharvey'

import multiprocessing
from corpus.mysql.reddit import RedditMySQLCorpus
import cred

if __name__ == '__main__':
    corpus = RedditMySQLCorpus(8)
    corpus.setup(**(cred.kwargs))
    corpus.create()
    corpus.gen_test_read()