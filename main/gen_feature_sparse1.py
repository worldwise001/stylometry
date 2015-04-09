__author__ = 'sharvey'

import multiprocessing

from corpus.mysql.reddit import RedditMySQLCorpus
from feature import ngram
from feature import lexical
import cred

from operator import itemgetter
import pprint
import re

def feature_to_numeric(features):
    corpus = RedditMySQLCorpus()
    corpus.setup(**(cred.kwargs))
    where_list = []
    for k in features:
        where_list.append(' (`type` = \'%s\' AND `feature` = \'%s\') '
                          % (k[0], k[1].replace('\\', '\\\\').replace('\n', '\\n').replace('\'', '\\\'')))
    numeric = {}
    for x in range(0, len(features), 100):
        where_clause = 'WHERE ' + '\n OR'.join(where_list[x:x+100])
        #print(where_clause)
        rows = corpus.run_sql('SELECT `id`, `type`, `feature` FROM `feature_map` '+where_clause, None)
        for row in rows:
            if (row['type'], row['feature']) not in features:
                continue
            numeric[row['id']] = features[(row['type'], row['feature'])]
    return numeric

def gen_feature(atuple):
    text = re.sub(r'https?://([a-zA-Z0-9\.\-_]+)[\w\-\._~:/\?#@!\$&\'\*\+,;=%%]*',
                  '\\1', atuple['text'], flags=re.MULTILINE)
    featurize = {}
    bngram = ngram.get_byte_ngrams(text)
    for n in bngram['ngram_byte']:
        for k in bngram['ngram_byte'][n]:
            featurize[('nb%d' % n, k)] = bngram['ngram_byte'][n][k]
    wngram = ngram.get_word_ngrams(text)
    for n in wngram['ngram_word_clean']:
        for k in wngram['ngram_word_clean'][n]:
             featurize[('nwc%d' % n, ' '.join(k))] = wngram['ngram_word_clean'][n][k]
    words = ngram.get_word_ngram(text, n=1, clean=False)
    words = { k[0]: words[k] for k in words}
    for word in words:
        featurize[('w', word)] = words[word]
    lex = lexical.get_symbol_dist(text)
    for k in lex['lex']:
        featurize[('l', k)] = lex['lex'][k]
    featurize = feature_to_numeric(featurize)
    featurize = [(k, featurize[k]) for k in featurize]
    featurize = sorted(featurize, key=itemgetter(0))
    vector = ' '.join(['%d:%d' % (i, j) for i, j in featurize])
    return (atuple['id'], vector)

if __name__ == '__main__':
    corpus = RedditMySQLCorpus()
    corpus.setup(**(cred.kwargs))
    corpus.create()
    pool = multiprocessing.Pool(16)
    print('set up pool')

    chunk = 100
    for reddit in ['worldnews', 'quantum', 'netsec', 'uwaterloo']:
        j = 0
        i = 0
        while True:
            print('j=%d' % j)
            rows = corpus.run_sql('SELECT `comment`.`id` AS `id`, `body` AS `text` FROM `comment` '
                                  'LEFT JOIN `submission` ON (`comment`.`submission_id`=`submission`.`id`) '
                                  'LEFT JOIN `reddit` ON (`submission`.`reddit_id`=`reddit`.`id`) '
                                  'WHERE `reddit`.`name`= \'%s\''
                                  'LIMIT %d, %d' % (reddit, j, chunk), None)
            if len(rows) == 0:
                break
            #for row in rows:
            #    atuple = gen_feature(row)
            #    pprint.pprint(atuple)
            it = pool.imap_unordered(gen_feature, rows)
            while True:
                try:
                    atuple = it.next()
                    corpus.run_sql('INSERT IGNORE INTO `comment_sparse_feature1` (`id`, `vector`) VALUES (%s, %s)',
                                   atuple)
                    i += 1
                    #print('i=%d' % i)
                except StopIteration:
                    break
            j += chunk
