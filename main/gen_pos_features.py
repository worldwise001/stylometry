__author__ = 'sharvey'

import multiprocessing

from corpus.mysql.reddit import RedditMySQLCorpus
from feature import ngram
from feature import lexical
import cred

import pprint
import re

def gen_feature(atuple):
    text = re.sub(r'https?://([a-zA-Z0-9\.\-_]+)[\w\-\._~:/\?#@!\$&\'\*\+,;=%%]*',
                  '\\1', atuple['text'], flags=re.MULTILINE)
    aset = set()
    wngram = ngram.get_word_ngrams(text)
    for n in wngram['ngram_word']:
        for k in wngram['ngram_word'][n]:
            aset.add(('npos%d' % n, ' '.join(k)))
    words, clean_words = ngram.get_words(text)
    for word in words:
        aset.add(('pos', word))
    return set(aset)

if __name__ == '__main__':
    corpus = RedditMySQLCorpus()
    corpus.setup(**(cred.kwargs))
    corpus.create()
    pool = multiprocessing.Pool(2)
    print('set up pool')

    chunk = 200
    feature_set = set()
    for reddit in ['worldnews', 'quantum', 'netsec', 'uwaterloo', 'gaming', 'news', 'AskReddit']:
        j = 0
        while True:
            print('j=%d' % j)
            rows = corpus.run_sql('SELECT `body_pos` AS `text` FROM `comment_pos` '
                                  'LEFT JOIN `comment` ON (`comment`.`id`=`comment_pos`.`id`) '
                                  'LEFT JOIN `submission` ON (`comment`.`submission_id`=`submission`.`id`) '
                                  'LEFT JOIN `reddit` ON (`submission`.`reddit_id`=`reddit`.`id`) '
                                  'WHERE `reddit`.`name`= \'%s\''
                                  'LIMIT %d, %d' % (reddit, j, chunk), None)
            if len(rows) == 0:
                break
            it = pool.imap_unordered(gen_feature, rows, 10)
            new_feature_set = set()
            while True:
                try:
                    atuple = it.next()
                    new_feature_set = new_feature_set.union(atuple)
                except StopIteration:
                    break
            print('calc difference')
            new_feature_set.difference_update(feature_set)
            print('difference calced')
            fp = open('/tmp/sharvey_pos_features', 'w')
            for f in list(new_feature_set):
                fp.write('\'%s\',\'%s\'\n' % (f[0], f[1].encode('utf-8').replace('\\', '\\\\').replace('\n', '\\n').replace('\'', '\\\'')))
            fp.close()
            query = 'LOAD DATA INFILE \'/tmp/sharvey_pos_features\' IGNORE INTO TABLE `feature_map_test` '\
                           'FIELDS TERMINATED BY \',\' OPTIONALLY ENCLOSED BY \'\\\'\' '\
                           'LINES TERMINATED BY \'\\n\' ' \
                           '(`type`, `feature`)'
            print(query)
            corpus.run_sql(query, None)
            feature_set = feature_set.union(new_feature_set)
            j += chunk
        if j % 5000 == 0:
            feature_set = set()
